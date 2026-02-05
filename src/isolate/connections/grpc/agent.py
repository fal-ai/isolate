# agent-requires: isolate[server]
"""
This file contains the implementation of the gRPC agent. The agent is a
separate process that is responsible for running the user code in a
sandboxed environment.

This file is referenced by the latest version of the `isolate` package
but then runs it in the context of the frozen agent built environment.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import os
import signal
import sys
import threading
import traceback
from argparse import ArgumentParser
from concurrent import futures
from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterator,
    Iterable,
    TextIO,
)

from grpc import StatusCode, aio, local_server_credentials

from isolate.connections.grpc.definitions import PartialRunResult

try:
    from isolate import __version__ as agent_version
except ImportError:
    agent_version = "UNKNOWN"

from isolate.backends.common import sha256_digest_of
from isolate.connections.common import SerializationError, serialize_object
from isolate.connections.grpc import definitions
from isolate.connections.grpc.configuration import get_default_options
from isolate.connections.grpc.interface import from_grpc

IDLE_TIMEOUT_SECONDS = int(os.getenv("ISOLATE_AGENT_IDLE_TIMEOUT_SECONDS", "0"))

isolate_log_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "ISOLATE_CONTEXT_VAR_LOG", default={}
)


def get_log_context() -> dict[str, Any]:
    """Extract the contextvar that is set to the log_context."""
    value = isolate_log_context.get()
    if not isinstance(value, dict):
        return {}

    return value


class JsonStdoutProxy:
    """
    A proxy around a real text stream (usually sys.__stdout__).
    - Intercepts write/writelines and emits JSON lines with contextvars
    - Delegates everything else to the wrapped stream to preserve compatibility.
    - Avoids recursion by always writing to the underlying stream.
    """

    def __init__(self, underlying):
        self._u = underlying
        self._local = threading.local()

    def write(self, s: str) -> int:
        # Many libs call write(""); keep semantics cheap.
        if not s:
            return 0

        # Prevent re-entrancy if something in encoding/IO calls back into sys.stdout.
        if getattr(self._local, "in_write", False):
            return self._u.write(s)

        self._local.in_write = True
        try:
            # Preserve "print" behavior: print() typically writes text
            # possibly with '\n'
            # Emit one JSON object per line. Keep partial lines buffered per-thread.
            buf = getattr(self._local, "buf", "")
            buf += s
            lines = buf.splitlines(keepends=True)

            out_count = 0
            new_buf = ""

            for chunk in lines:
                if chunk.endswith("\n") or chunk.endswith("\r\n"):
                    msg = chunk.rstrip("\r\n")
                    payload = self._format_record(msg)
                    out_count += self._u.write(payload + "\n")
                else:
                    # Incomplete line: keep buffering
                    new_buf += chunk

            self._local.buf = new_buf
            return len(s)
        finally:
            self._local.in_write = False

    def flush(self) -> None:
        # Flush any buffered partial line as a final record (optional choice).
        if getattr(self._local, "in_write", False):
            return self._u.flush()

        buf = getattr(self._local, "buf", "")
        if buf:
            self._local.buf = ""
            payload = self._format_record(buf)
            self._u.write(payload + "\n")
        self._u.flush()

    def writelines(self, lines: list[str]) -> None:
        for line in lines:
            self.write(line)

    def _format_record(self, message: str) -> str:
        record = {
            "line": message,
        }
        # Add the log context to the json so we propagate contextvars
        record.update(get_log_context())
        return json.dumps(record, ensure_ascii=False)

    def __getattr__(self, name: str) -> Any:
        # Delegate missing attrs/methods to underlying stream
        # (isatty, fileno, encoding, etc.)
        return getattr(self._u, name)

    def __iter__(self):
        return iter(self._u)

    def __enter__(self):
        self._u.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._u.__exit__(exc_type, exc, tb)


@dataclass
class AbortException(Exception):
    message: str


class AgentServicer(definitions.AgentServicer):
    def __init__(self, log_file: TextIO | None = None):
        super().__init__()

        self._run_cache: dict[str, Any] = {}
        self._log = log_file if log_file is not None else sys.stdout
        self._thread_pool = futures.ThreadPoolExecutor(max_workers=1)
        self._idle_timeout_seconds = IDLE_TIMEOUT_SECONDS
        self._is_running = asyncio.Event()
        self._is_idle = asyncio.Event()
        self._is_idle.set()

        def handle_sigint(*args):
            self.log("SIGINT signal received, shutting down...")
            signal.raise_signal(signal.SIGTERM)

        signal.signal(signal.SIGINT, handle_sigint)

    async def wait_for_idle_timeout(self) -> None:
        while True:
            # print(f"Hello, world! {self._idle_timeout_seconds}")
            # wait for the agent to be idle
            await self._is_idle.wait()

            # idle timeout disabled
            if self._idle_timeout_seconds <= 0:
                # prevent blocking the event loop
                await asyncio.sleep(0.1)
                continue

            try:
                # wait for the agent to be running
                await asyncio.wait_for(
                    self._is_running.wait(), timeout=self._idle_timeout_seconds
                )
            except asyncio.TimeoutError:
                self.log(
                    f"Idle timeout {self._idle_timeout_seconds} seconds exceeded, shutting down..."  # noqa: E501
                )
                # This kills the agent itself, however it will remain as a zombie state
                # unless the parent process (server) properly handles the SIGCHLD.
                signal.raise_signal(signal.SIGTERM)
                break
            except asyncio.CancelledError:
                # Cancelled when the server is shutting down
                break

    async def Run(
        self,
        request: definitions.FunctionCall,
        context: aio.ServicerContext,
    ) -> AsyncIterator[PartialRunResult]:
        self._is_idle.clear()
        self._is_running.set()
        try:
            async for result in self._Run(request, context):
                yield result
        finally:
            self._is_running.clear()
            self._is_idle.set()

    async def _Run(
        self,
        request: definitions.FunctionCall,
        context: aio.ServicerContext,
    ) -> AsyncIterator[PartialRunResult]:
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
                    ) = await self.execute_function(
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
                    self.abort_with_msg(context, exc.message)
                    return
                else:
                    assert not was_it_raised
                    self._run_cache[cache_key] = result

            extra_args.append(self._run_cache[cache_key])

        try:
            result, was_it_raised, stringized_tb = await self.execute_function(
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
            self.abort_with_msg(context, exc.message)
            return

    async def execute_function(
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
        except SerializationError as exc:
            str_tb = traceback.format_exc()
            self.log(str_tb)
            self.log(f"The {function_kind} function could not be deserialized.")
            return exc, True, str_tb

        if not callable(function):
            raise AbortException(
                f"The {function_kind} function must be callable, "
                f"not {type(function).__name__}."
            )

        self.log(f"Starting the execution of the {function_kind} function.")

        was_it_raised = False
        stringized_tb = None
        try:
            # Newer fal SDK will mark async entrypoints with `_run_on_main_thread` so
            # we execute on the main loop and can await the coroutine they return.
            # Older fal SDK still call `asyncio.run(...)`.
            # To avoid error "asyncio.run() cannot be called from a running event loop"
            # and be backward compatible,
            # we offload those unflagged functions to a thread pool.

            if getattr(function, "_run_on_main_thread", False):
                result = function(*extra_args)
            else:
                result = self._thread_pool.submit(function, *extra_args).result()

            if asyncio.iscoroutine(result):
                result = await result

        except BaseException as exc:
            result = exc
            was_it_raised = True
            num_frames = len(traceback.extract_stack()[:-5])
            stringized_tb = "".join(traceback.format_exc(limit=-num_frames))

        if not was_it_raised:
            self.log(f"Completed the execution of the {function_kind} function.")
        else:
            self.log(
                f"Completed the execution of the {function_kind} function"
                f" with an error: {result}\nTraceback:\n{stringized_tb}"
            )
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
        context: aio.ServicerContext,
        message: str,
        *,
        code: StatusCode = StatusCode.INVALID_ARGUMENT,
    ) -> None:
        context.set_code(code)
        context.set_details(message)
        return None


def create_server(address: str) -> aio.Server:
    """Create a new (temporary) gRPC server listening on the given
    address."""
    # Use asyncio server so requests can run in the main thread and intercept signals
    # There seems to be a weird bug with grpcio that makes subsequent requests fail with
    # concurrent rpc limit exceeded if we set maximum_current_rpcs to 1. Setting it to 2
    # fixes it, even though in practice, we only run one request at a time.
    server = aio.server(
        maximum_concurrent_rpcs=2,
        options=get_default_options(),
    )

    # Local server credentials allow us to ensure that the
    # connection is established by a local process.
    server_credentials = local_server_credentials()
    server.add_secure_port(address, server_credentials)
    return server


async def run_agent(
    address: str, log_fd: int | None = None, json_logs: bool = False
) -> int:
    """Run the agent servicer on the given address."""
    # Determine the base log file
    if log_fd is None:
        log_file: TextIO = sys.stdout
    else:
        log_file = os.fdopen(log_fd, "w")

    # Apply JSON wrapper if requested
    if json_logs:
        sys.stdout = JsonStdoutProxy(sys.__stdout__)  # type: ignore[assignment]
        sys.stderr = JsonStdoutProxy(sys.__stderr__)  # type: ignore[assignment]
        log_file = JsonStdoutProxy(log_file)  # type: ignore[assignment]

    server = create_server(address)
    servicer = AgentServicer(log_file=log_file)

    # This function just calls some methods on the server
    # and register a generic handler for the bridge. It does
    # not have any global side effects.
    definitions.register_agent(servicer, server)

    await server.start()

    _, pending = await asyncio.wait(
        [
            asyncio.create_task(server.wait_for_termination()),
            asyncio.create_task(servicer.wait_for_idle_timeout()),
        ],
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        print(f"Cancelling task: {task}")
        task.cancel()

    return 0


async def main() -> int:
    parser = ArgumentParser()
    parser.add_argument("address", type=str)
    parser.add_argument("--log-fd", type=int)
    parser.add_argument("--json-logs", action="store_true", default=False)

    options = parser.parse_args()
    return await run_agent(
        options.address, log_fd=options.log_fd, json_logs=options.json_logs
    )


if __name__ == "__main__":
    asyncio.run(main())
