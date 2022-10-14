import os
from argparse import ArgumentParser
from concurrent import futures
from typing import Iterator

import grpc

from isolate.backends import BaseEnvironment
from isolate.backends.local import LocalPythonEnvironment
from isolate.server import definitions
from isolate.server.controller import LocalPythonRPC
from isolate.server.serialization import from_grpc

# Whether to inherit the packages from the local environment or not.
INHERIT_FROM_LOCAL = os.getenv("ISOLATE_INHERIT_FROM_LOCAL") == "1"

# Number of threads that the gRPC server will use.
MAX_THREADS = os.getenv("MAX_THREADS", 5)


class IsolateServicer(definitions.IsolateServicer):
    def Run(
        self,
        request: definitions.BoundFunction,
        context: grpc.ServicerContext,
    ) -> Iterator[definitions.PartialRunResult]:
        if request.function.was_it_raised:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("The bound function can not be a raised exception.")
            return None

        try:
            environment = from_grpc(request.environment, BaseEnvironment)
        except ValueError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"Environment creation error: {exc}.")
            return None

        extra_inheritance_paths = []
        if INHERIT_FROM_LOCAL:
            local_environment = LocalPythonEnvironment()
            extra_inheritance_paths.append(local_environment.create())

        connection = environment.create()
        with LocalPythonRPC(
            environment,
            connection,
            extra_inheritance_paths=extra_inheritance_paths,
        ) as connection:
            yield from connection.proxy_grpc(request.function)


def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    definitions.register_isolate(IsolateServicer(), server)

    grpc.alts_server_credentials()
    server.add_insecure_port(f"[::]:50001")

    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    main()
