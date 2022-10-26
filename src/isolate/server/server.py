import os
import traceback
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from functools import partial
from queue import Empty as QueueEmpty
from queue import Queue
from typing import Callable, Iterator, cast

import grpc
from grpc import ServicerContext, StatusCode

from isolate.backends import EnvironmentCreationError, IsolateSettings
from isolate.backends.local import LocalPythonEnvironment
from isolate.connections.grpc import AgentError, LocalPythonGRPC
from isolate.logs import Log, LogLevel, LogSource
from isolate.server import definitions
from isolate.server.interface import from_grpc, to_grpc

# Whether to inherit all the packages from the current environment or not.
INHERIT_FROM_LOCAL = os.getenv("ISOLATE_INHERIT_FROM_LOCAL") == "1"

# Number of threads that the gRPC server will use.
MAX_THREADS = int(os.getenv("MAX_THREADS", 5))

# Number of seconds to observe the queue before checking the termination
# event.
_Q_WAIT_DELAY = 0.1


@dataclass
class IsolateServicer(definitions.IsolateServicer):
    default_settings: IsolateSettings = field(default_factory=IsolateSettings)

    def Run(
        self,
        request: definitions.BoundFunction,
        context: ServicerContext,
    ) -> Iterator[definitions.PartialRunResult]:
        messages: Queue[definitions.PartialRunResult] = Queue()
        try:
            environment = from_grpc(request.environment)
        except ValueError as exc:
            return self.abort_with_msg(
                f"Unknown environment kind: {request.environment.kind}.",
                context,
            )

        run_settings = replace(
            self.default_settings,
            log_hook=partial(_add_log_to_queue, messages),
            serialization_method=request.function.method,
        )
        environment.apply_settings(run_settings)

        extra_inheritance_paths = []
        if INHERIT_FROM_LOCAL:
            local_environment = LocalPythonEnvironment()
            extra_inheritance_paths.append(local_environment.create())

        with ThreadPoolExecutor(max_workers=1) as local_pool:
            future = local_pool.submit(environment.create)

            yield from self.watch_queue_until_completed(messages, future.done)
            try:
                # Assuming that the iterator above only stops yielding once
                # the future is completed, the timeout here should be redundant
                # but it is just in case.
                connection = future.result(timeout=0.1)
            except EnvironmentCreationError:
                return self.abort_with_msg(
                    f"A problem occurred while creating the environment.",
                    context,
                )

            with LocalPythonGRPC(
                environment,
                connection,
                extra_inheritance_paths=extra_inheritance_paths,
            ) as connection:
                future = local_pool.submit(
                    _proxy_to_queue,
                    queue=messages,
                    connection=connection,
                    input=request.function,
                )

                # Unlike above; we are not interested in the result value of future
                # here, since it will be already transferred to other side without
                # us even seeing (through the queue).
                yield from self.watch_queue_until_completed(messages, future.done)

                # But we still have to check whether there were any errors raised
                # during the execution, and handle them accordingly.
                exception = future.exception(timeout=0.1)
                if exception is not None:
                    for line in traceback.format_exception(
                        type(exception), exception, exception.__traceback__
                    ):
                        yield from self.log(line, level=LogLevel.ERROR)
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
        level: LogLevel = LogLevel.TRACE,
        source: LogSource = LogSource.BRIDGE,
    ) -> Iterator[definitions.PartialRunResult]:
        log = to_grpc(Log(message, level=level, source=source))
        log = cast(definitions.Log, log)
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


def _proxy_to_queue(
    queue: Queue,
    connection: LocalPythonGRPC,
    input: definitions.SerializedObject,
) -> None:
    for message in connection._run_through_grpc(input):
        queue.put_nowait(message)


def _add_log_to_queue(messages: Queue, log: Log) -> None:
    grpc_log = cast(definitions.Log, to_grpc(log))
    grpc_result = definitions.PartialRunResult(
        is_complete=False,
        logs=[grpc_log],
        result=None,
    )
    messages.put_nowait(grpc_result)


def main() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=MAX_THREADS))
    definitions.register_isolate(IsolateServicer(), server)

    server.add_insecure_port(f"[::]:50001")
    print("Started listening at localhost:50001")

    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    main()
