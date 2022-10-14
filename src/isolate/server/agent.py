# agent-requires: isolate[server]

from __future__ import annotations

import os
import site

# WARNING: All the actual imports must go after the load_pth_files() call.


def load_pth_files() -> None:
    """Each site dir in Python can contain some .pth files, which are
    basically instructions that tell Python to load other stuff. This is
    generally used for editable installations, and just setting PYTHONPATH
    won't make them expand so we need manually process them. Luckily, site
    module can simply take the list of new paths and recognize them.

    https://docs.python.org/3/tutorial/modules.html#the-module-search-path
    """
    python_path = os.getenv("PYTHONPATH")
    if python_path is None:
        return None

    # TODO: The order here is the same as the one that is used for generating the
    # PYTHONPATH. The only problem that might occur is that, on a chain with
    # 3 ore more nodes (A, B, C), if X is installed as an editable package to
    # B and a normal package to C, then C might actually take precedence. This
    # will need to be fixed once we are dealing with more than 2 nodes and editable
    # packages.
    for site_dir in python_path.split(os.pathsep):
        site.addsitedir(site_dir)


if __name__ == "__main__":
    load_pth_files()

from argparse import ArgumentParser
from concurrent import futures
from dataclasses import dataclass
from typing import Iterator

import grpc
from grpc import ServicerContext

from isolate.server import definitions
from isolate.server.serialization import from_grpc, to_grpc


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
