# agent-requires: isolate[server]

from __future__ import annotations

from argparse import ArgumentParser
from concurrent import futures
from dataclasses import dataclass
from typing import Iterator

import grpc
from grpc import ServicerContext

from isolate.server_v2 import definitions
from isolate.server_v2.serialization import from_grpc, to_grpc


@dataclass
class AgentServicer(definitions.AgentServicer):
    def Run(
        self,
        request: definitions.SerializedObject,
        context: ServicerContext,
    ) -> Iterator[definitions.PartialRunResult]:
        print(f"A connection has been established: {context.peer()}!")

        # validate: not request.was_it_raised

        function = from_grpc(request, object)

        # validate: no SerializationError
        # validate: callable(function)

        print("Serialized the function.")
        was_it_raised = False
        try:
            result = function()
        except BaseException as exc:
            result = exc
            was_it_raised = True

        print("Executed the function, serializing the result.")
        # validate: no SerializationError
        serialized_result = to_grpc(
            result,
            definitions.SerializedObject,
            method=request.method,
            was_it_raised=was_it_raised,
        )

        print("Sending the result back.")
        yield definitions.PartialRunResult(
            result=serialized_result, is_complete=True, logs=[]
        )


def create_server(address: str) -> grpc.Server:
    """Create a new (temporary) gRPC server listening on the given
    address."""
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=1),
        maximum_concurrent_rpcs=1,
    )

    # Local server credentials allow us to ensure that the
    # connection is established by a local process.
    server_credentials = grpc.local_server_credentials()
    server.add_secure_port(address, server_credentials)
    return server


def run_agent(address: str) -> int:
    """Run the agent servicer on the given address."""
    server = create_server(address)
    servicer = AgentServicer()

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

    options = parser.parse_args()
    return run_agent(options.address)


if __name__ == "__main__":
    main()
