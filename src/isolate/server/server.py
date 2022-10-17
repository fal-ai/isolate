import os
import threading
import traceback
from argparse import ArgumentParser
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import Empty as QueueEmpty
from queue import Queue
from typing import Any, Callable, Iterator, Optional, Tuple

import grpc
from grpc import ServicerContext, StatusCode

from isolate.backends import BaseEnvironment, EnvironmentCreationError
from isolate.backends.context import GLOBAL_CONTEXT
from isolate.backends.local import LocalPythonEnvironment
from isolate.server import definitions
from isolate.server.controller import AgentError, LocalPythonRPC
from isolate.server.serialization import from_grpc, to_grpc

# Whether to inherit the packages from the local environment or not.
INHERIT_FROM_LOCAL = os.getenv("ISOLATE_INHERIT_FROM_LOCAL") == "1"

# Number of threads that the gRPC server will use.
MAX_THREADS = os.getenv("MAX_THREADS", 5)

# Number of seconds to observe the queue before checking the termination
# event.
_Q_WAIT_DELAY = 0.1


@dataclass
class ExceptionBox:
    exception: Optional[BaseException] = None
    traceback: Optional[str] = None


class IsolateServicer(definitions.IsolateServicer):
    def Run(
        self,
        request: definitions.BoundFunction,
        context: ServicerContext,
    ) -> Iterator[definitions.PartialRunResult]:
        messages = Queue()
        try:
            environment = from_grpc(request.environment, BaseEnvironment)
        except ValueError as exc:
            return self.abort_with_msg(f"Environment creation error: {exc}.", context)

        run_ctx = GLOBAL_CONTEXT._replace(
            _log_handler=lambda log: messages.put_nowait(
                definitions.PartialRunResult(
                    is_complete=False,
                    logs=[to_grpc(log, definitions.Log)],
                    result=None,
                )
            ),
            _serialization_backend=request.function.method,
        )
        environment.set_context(run_ctx)

        extra_inheritance_paths = []
        if INHERIT_FROM_LOCAL:
            local_environment = LocalPythonEnvironment()
            extra_inheritance_paths.append(local_environment.create())

        local_pool = ThreadPoolExecutor(max_workers=1)
        future = local_pool.submit(environment.create)

        yield from self.watch_queue_until_completed(messages, future.done)
        try:
            # Assuming that the iterator above only stops yielding once
            # the future is completed, the timeout here should be redundant
            # but it is just in case.
            connection = future.result(timeout=0.1)
        except EnvironmentCreationError:
            return self.abort_with_msg(
                f"A problem occurred while creating the environment.", context
            )

        with LocalPythonRPC(
            environment,
            connection,
            extra_inheritance_paths=extra_inheritance_paths,
        ) as connection:
            future = local_pool.submit(
                lambda queue: [
                    queue.put_nowait(message)
                    for message in connection.proxy_grpc(request.function)
                ],
                queue=messages,
            )

            yield from self.watch_queue_until_completed(messages, future.done)
            exception = future.exception(timeout=0.1)
            if exception is not None:
                for line in traceback.format_exception(
                    type(exception), exception, exception.__traceback__
                ):
                    yield from self.log(line)
                if isinstance(exception, AgentError):
                    return self.abort_with_msg(
                        str(exception),
                        context,
                        code=StatusCode.ABORTED,
                    )
                else:
                    return self.abort_with_msg(
                        f"An unexpected error occurred.",
                        context,
                        code=StatusCode.UNKNOWN,
                    )

    def watch_queue_until_completed(
        self, queue: Queue, is_completed: Callable[[], bool]
    ) -> Iterator[definitions.PartialRunResult]:
        """Watch the given queue until the is_completed function returns True. Note that even
        if the function is completed, this function might not finish until the queue is empty."""
        while not is_completed():
            try:
                yield queue.get(timeout=_Q_WAIT_DELAY)
            except QueueEmpty:
                continue

        # Clear the final messages
        while not queue.empty():
            try:
                yield queue.get_nowait()
            except QueueEmpty:
                continue

    def log(
        self,
        message: str,
        level: definitions.LogLevel = definitions.TRACE,
        source: definitions.LogSource = definitions.BRIDGE,
    ) -> Iterator[definitions.PartialRunResult]:
        log = definitions.Log(message=message, level=level, source=source)
        yield definitions.PartialRunResult(result=None, is_complete=False, logs=[log])

    def abort_with_msg(
        self,
        message: str,
        context: ServicerContext,
        *,
        code: StatusCode = StatusCode.INVALID_ARGUMENT,
    ) -> None:
        context.set_code(code)
        context.set_details(message)
        return None


def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    definitions.register_isolate(IsolateServicer(), server)

    grpc.alts_server_credentials()
    server.add_insecure_port(f"[::]:50001")

    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    main()
