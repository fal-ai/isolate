import logging
import os
import queue
import threading
import time
import traceback
from concurrent import futures
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Iterator, List

import grpc

from isolate.backends import (
    BaseEnvironment,
    EnvironmentCreationError,
    UserException,
)
from isolate.backends.context import GLOBAL_CONTEXT, Log, LogSource
from isolate.registry import prepare_environment
from isolate.server import definitions
from isolate.server._runs import (
    SECONDARY_SERIALIZATION_METHOD,
    run_serialized_function_in_env,
)
from isolate.server._serialization import from_grpc, to_grpc

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
GRPC_ADDRESS = os.getenv("GRPC_ACCESS", "[::]:50051")

PER_ITERATION_DELAY = 0.1
MAX_JOIN_TIMEOUT = 2.5

logger = logging.getLogger(__name__)


@dataclass
class RuntimeManager:
    environment: BaseEnvironment = field(repr=False)
    serialization_method: str = field(repr=False)
    done_signal: threading.Event = field(default_factory=threading.Event)
    log_queue: queue.Queue[Log] = field(default_factory=queue.Queue)

    def __post_init__(self) -> None:
        run_ctx = GLOBAL_CONTEXT._replace(
            _serialization_backend=self.serialization_method,
            _log_handler=self.log_queue.put_nowait,
        )
        self.environment.set_context(run_ctx)

    def relay_logs(self) -> Iterator[definitions.PartialRunResult]:
        buffered_logs = []
        with suppress(queue.Empty):
            while not self.log_queue.empty():
                buffered_logs.append(
                    to_grpc(self.log_queue.get_nowait(), definitions.Log)
                )

        if buffered_logs:
            yield definitions.PartialRunResult(
                is_complete=False,
                log=buffered_logs,
            )

    def run(self, dehydrated_object: bytes) -> Iterator[definitions.PartialRunResult]:
        """Run a dehydrated object in the specified environment."""
        thread = threading.Thread(
            target=run_serialized_function_in_env,
            args=(self.environment, dehydrated_object, self.done_signal),
        )
        thread.start()
        while not self.done_signal.is_set():
            yield from self.relay_logs()
            # Prevent busy looping by sleeping for a bit
            # and releasing the GIL.
            time.sleep(PER_ITERATION_DELAY)

        thread.join(MAX_JOIN_TIMEOUT)
        yield from self.relay_logs()

        # TODO: return the actual result
        return None


class BridgeServicer(definitions.BridgeServicer):
    def Run(
        self, request: definitions.BoundFunction, context: grpc.ServicerContext
    ) -> Iterator[definitions.PartialRunResult]:
        if request.function.is_exception:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("The bound function can not be an exception.")
            return None

        try:
            environment = from_grpc(request.environment, BaseEnvironment)
        except ValueError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"Environment creation error: {exc}.")
            return None

        manager = RuntimeManager(
            environment=environment,
            serialization_method=request.function.serialization_method,
        )

        try:
            result = yield from manager.run(request.function.definition)
        except EnvironmentCreationError:
            context.set_code(grpc.StatusCode.ABORTED)
            context.set_details(f"Environment creation error: {exc}.")
            return None
        except BaseException as exc:
            # This can only happen if something goes wrong with isolate's
            # code (possibly due to a bug in us, no user fault).
            logger.error("Unknown error in isolate server.")
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(f"Isolate server error: {exc}.")
            return None

        logs = []
        try:
            serialized_result = to_grpc(
                result,
                definitions.SerializedObject,
                serialization_method=SECONDARY_SERIALIZATION_METHOD,
                is_exception=False,
            )
        except BaseException as exc:
            # If we can't serialize the end result with the custom backend
            # or if if the result has an unserializable object then just reply
            # without any result.
            serialized_result = None
            logs.append(
                to_grpc(
                    Log(
                        f"Failed to serialize the result object."
                        f"\n{traceback.format_exc()}",
                        LogSource.BRIDGE,
                    ),
                    definitions.Log,
                )
            )

        yield definitions.PartialRunResult(
            is_complete=True,
            log=logs,
            result=serialized_result,
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    definitions.register_servicer(BridgeServicer(), server)

    if ACCESS_TOKEN:
        credentials = grpc.access_token_call_credentials(ACCESS_TOKEN)
        server.add_secure_port(f"{GRPC_ADDRESS}", credentials)
    else:
        server.add_insecure_port(f"{GRPC_ADDRESS}")

    logging.info("Starting isolate server on %s", GRPC_ADDRESS)
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    serve()
