# agent-requires: isolate[server]
"""
This file contains the implementation of the gRPC agent. The agent is a
separate process that is responsible for running the user code in a
sandboxed environment.

This file is referenced by the latest version of the `isolate` package
but then runs it in the context of the frozen agent built environment.
"""

from __future__ import annotations

import os
import sys
import traceback
from argparse import ArgumentParser
from concurrent import futures
from dataclasses import dataclass
from typing import (
    Any,
    Iterable,
    Iterator,
)

import grpc
from grpc import ServicerContext, StatusCode

try:
    from isolate import __version__ as agent_version
except ImportError:
    agent_version = "UNKNOWN"

from isolate.backends.common import sha256_digest_of
from isolate.connections.common import SerializationError, serialize_object
from isolate.connections.grpc import definitions
from isolate.connections.grpc.configuration import get_default_options
from isolate.connections.grpc.interface import from_grpc


@dataclass
class AbortException(Exception):
    message: str


class AgentServicer(definitions.AgentServicer):
    def __init__(self, log_fd: int | None = None):
        super().__init__()

        self._run_cache: dict[str, Any] = {}
        self._log = sys.stdout if log_fd is None else os.fdopen(log_fd, "w")

    def Run(
        self,
        request: definitions.FunctionCall,
        context: ServicerContext,
    ) -> Iterator[definitions.PartialRunResult]:
        self.log(f"A connection has been established: {context.peer()}!")
        server_version = os.getenv("ISOLATE_SERVER_VERSION") or "unknown"
        self.log(f"Isolate info: server {server_version}, agent {agent_version}")

        extra_args = []
        if request.HasField("setup_func"):
            cache_key = sha256_digest_of(
                request.setup_func.definition,
                request.setup_func.method,
            )
            if cache_key not in self._run_cache:
                try:
                    (
                        result,
                        was_it_raised,
                        stringized_tb,
                    ) = self.execute_function(
                        request.setup_func,
                        "setup",
                    )

                    if was_it_raised:
                        self.log(
                            "The setup function has thrown an error. Aborting the run."
                        )
                        yield self.send_object(
                            request.setup_func.method,
                            result,
                            was_it_raised,
                            stringized_tb,
                        )
                        raise AbortException("The setup function has thrown an error.")
                except AbortException as exc:
                    return self.abort_with_msg(context, exc.message)
                else:
                    assert not was_it_raised
                    self._run_cache[cache_key] = result

            extra_args.append(self._run_cache[cache_key])

        try:
            result, was_it_raised, stringized_tb = self.execute_function(
                request.function,
                "function",
                extra_args=extra_args,
            )
            yield self.send_object(
                request.function.method,
                result,
                was_it_raised,
                stringized_tb,
            )
        except AbortException as exc:
            return self.abort_with_msg(context, exc.message)

    def execute_function(
        self,
        function: definitions.SerializedObject,
        function_kind: str,
        *,
        extra_args: Iterable[Any] = (),
    ) -> tuple[Any, bool, str | None]:
        if function.was_it_raised:
            raise AbortException(
                f"The {function_kind} function must be callable, "
                "not a raised exception."
            )

        try:
            # TODO: technically any sort of exception could be raised here, since
            # depickling is basically involves code execution from the *user*.
            function = from_grpc(function)
        except SerializationError:
            traceback.print_exc()
            raise AbortException(
                f"The {function_kind} function could not be deserialized."
            )

        if not callable(function):
            raise AbortException(
                f"The {function_kind} function must be callable, "
                f"not {type(function).__name__}."
            )

        self.log(f"Starting the execution of the {function_kind} function.")

        was_it_raised = False
        stringized_tb = None
        try:
            result = function(*extra_args)
        except BaseException as exc:
            result = exc
            was_it_raised = True
            num_frames = len(traceback.extract_stack()[:-5])
            stringized_tb = "".join(traceback.format_exc(limit=-num_frames))

        self.log(f"Completed the execution of the {function_kind} function.")
        return result, was_it_raised, stringized_tb

    def send_object(
        self,
        serialization_method: str,
        result: object,
        was_it_raised: bool,
        stringized_tb: str | None,
    ) -> definitions.PartialRunResult:
        try:
            definition = serialize_object(serialization_method, result)
        except SerializationError:
            if stringized_tb:
                print(stringized_tb, file=sys.stderr)
            self.log(traceback.format_exc())
            raise AbortException(
                "Error while serializing the execution result "
                f"(object of type {type(result)})."
            )
        except BaseException:
            self.log(traceback.format_exc())
            raise AbortException(
                "An unexpected error occurred while serializing the result."
            )

        self.log("Sending the result.")
        serialized_obj = definitions.SerializedObject(
            method=serialization_method,
            definition=definition,
            was_it_raised=was_it_raised,
            stringized_traceback=stringized_tb,
        )
        return definitions.PartialRunResult(
            result=serialized_obj,
            is_complete=True,
            logs=[],
        )

    def log(self, message: str) -> None:
        self._log.write(message + "\n")
        self._log.flush()

    def abort_with_msg(
        self,
        context: ServicerContext,
        message: str,
        *,
        code: StatusCode = StatusCode.INVALID_ARGUMENT,
    ) -> None:
        context.set_code(code)
        context.set_details(message)
        return None


def create_server(address: str) -> grpc.Server:
    """Create a new (temporary) gRPC server listening on the given
    address."""
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=1),
        maximum_concurrent_rpcs=1,
        options=get_default_options(),
    )

    # Local server credentials allow us to ensure that the
    # connection is established by a local process.
    server_credentials = grpc.local_server_credentials()
    server.add_secure_port(address, server_credentials)
    return server


def run_agent(address: str, log_fd: int | None = None) -> int:
    """Run the agent servicer on the given address."""
    server = create_server(address)
    servicer = AgentServicer(log_fd=log_fd)

    # This function just calls some methods on the server
    # and register a generic handler for the bridge. It does
    # not have any global side effects.
    definitions.register_agent(servicer, server)

    server.start()
    server.wait_for_termination()
    return 0


def main() -> int:
    parser = ArgumentParser()
    parser.add_argument("address", type=str)
    parser.add_argument("--log-fd", type=int)

    options = parser.parse_args()
    return run_agent(options.address, log_fd=options.log_fd)


if __name__ == "__main__":
    main()
