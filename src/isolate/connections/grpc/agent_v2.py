# Agent:
#   -> /CreateTask
#       Takes the function to execute, and puts into the queue
#   -> /CheckTask
#       Finds the tasks state (queued, running, finished) and returns the result
#   -> /Status
#       How many tasks are enqueued and running.
#   -> /Finalize
#       Stop receiving new tasks, and wait for all tasks to finish.
#   -> /Shutdown
#       Exit immediately (assumes all tasks are finished).

# Agent configuration:
#   PRIVILIGE_TOKEN: The authentication key to verify the incoming requests from the controller.
#   IDLE_BRINGUP_TIME: How long to wait before exiting the agent since the last call.
from __future__ import annotations

import queue
import secrets
import time
import traceback
from concurrent import futures
from contextlib import contextmanager
from dataclasses import dataclass, field
from queue import Queue
from threading import Event
from typing import Any, Callable, Generic, Iterator, Optional, TypeVar

import grpc
from grpc import ServicerContext, StatusCode

from isolate.connections.grpc import definitions as proto
from isolate.connections.grpc.configuration import get_default_options
from isolate.connections.grpc.interface import (
    from_grpc,
    to_grpc,
    to_serialized_object,
)

ResultT = TypeVar("ResultT")
SERIALIZATION_METHOD = "pickle"


def _log(message):
    print(f"[AGENT] {message}")


@dataclass
class AgentError(Exception):
    message: str


def _deserialize_and_run(
    serialized_function: proto.SerializedObject,
) -> Callable[[], ResultT]:
    def wrapper(*args, **kwargs):
        try:
            function = from_grpc(serialized_function)
        except BaseException as exc:
            raise AgentError(
                "Failed to deserialize the user provided function"
            ) from exc

        return function(*args, **kwargs)

    return wrapper


def _serialization_fault(exception: BaseException) -> proto.SerializedObject:
    raise NotImplementedError(
        "TODO: We probably need to send a meaningful error message here."
    )


@dataclass
class ExecutionResult:
    result: Any

    def serialize(self, method: str = SERIALIZATION_METHOD) -> proto.SerializedObject:
        return to_serialized_object(
            self.result,
            method=method,
            was_it_raised=False,
            stringized_traceback=None,
        )


@dataclass
class ExecutionError(ExecutionResult):
    stringized_tb: str

    def serialize(self, method: str = SERIALIZATION_METHOD) -> proto.SerializedObject:
        return to_serialized_object(
            self.result,
            method=method,
            was_it_raised=True,
            stringized_traceback=self.stringized_tb,
        )


@dataclass
class Task(Generic[ResultT]):
    id: str
    executable: Callable[[], ResultT]

    def cancel(self):
        self.is_cancelled = True

    def execute(self) -> ExecutionResult:
        try:
            result = ExecutionResult(self.executable())
        except BaseException as exc:
            num_frames = len(traceback.extract_stack()[:-5])
            stringized_tb = "".join(traceback.format_exc(limit=-num_frames))
            result = ExecutionError(exc, stringized_tb=stringized_tb)

        return result


@dataclass
class AgentV2Servicer(proto.AgentV2Servicer):
    tasks: dict[str, Task[Any]] = field(default_factory=dict)
    task_queue: Queue[str] = field(default_factory=Queue)
    active_task: Optional[Task[Any]] = None
    completed_tasks: dict[str, ExecutionResult] = field(default_factory=dict)
    idle_since: float | None = None

    def process_tasks(self, event: Event) -> None:
        # Check the finalization event in every 50ms
        # as long as there is nothing being executed.
        __event_check_timeout = 0.05
        while not event.is_set():
            try:
                task_id = self.task_queue.get(timeout=__event_check_timeout)
                task = self.tasks.pop(task_id)
            except queue.Empty:
                continue
            except KeyError:
                _log(f"Task {task_id} was probably cancelled before it was executed.")
                continue

            with self.switch_context(task):
                self.completed_tasks[task.id] = task.execute()

    @contextmanager
    def switch_context(self, task: Task[Any]) -> Iterator[None]:
        try:
            self.active_task, self.idle_since = task, None
            yield
        finally:
            self.active_task, self.idle_since = None, time.monotonic()

    def CreateTask(
        self,
        request: proto.CreateTaskRequest,
        context: ServicerContext,
    ) -> proto.CreateTaskResponse:
        task = Task(secrets.token_hex(4), _deserialize_and_run(request.function))
        self.tasks[task.id] = task
        self.task_queue.put(task.id)
        return proto.CreateTaskResponse(task_id=task.id)

    def CancelTaskRequest(
        self,
        request: proto.CancelTaskRequest,
        context: ServicerContext,
    ) -> proto.CancelTaskResponse:
        task = self.tasks.pop(request.task_id, None)
        return proto.CancelTaskResponse(did_cancel=task is not None)

    def CheckTask(
        self,
        request: proto.CheckTaskRequest,
        context: ServicerContext,
    ) -> proto.CheckTaskResponse:
        if request.task_id in self.completed_tasks:
            raw_result = self.completed_tasks[request.task_id]
            try:
                serialized_result = raw_result.serialize()
            except BaseException as exc:
                serialized_result = _serialization_fault(exc)

            return proto.CheckTaskResponse(
                status=proto.TaskStatus.COMPLETED, result=serialized_result
            )
        elif self.active_task and self.active_task.id == request.task_id:
            return proto.CheckTaskResponse(status=proto.TaskStatus.RUNNING)
        else:
            queued_tasks = self.tasks.keys()
            assert request.task_id in queued_tasks
            return proto.CheckTaskResponse(status=proto.TaskStatus.QUEUED)

    def AgentStatus(
        self,
        request: proto.AgentStatusRequest,
        context: ServicerContext,
    ) -> proto.AgentStatusResponse:
        idle_since = self.idle_since
        if idle_since is None:
            idle_for = 0.0
        else:
            idle_for = time.monotonic() - idle_since
        return proto.AgentStatusResponse(
            num_queued=len(self.tasks),
            idle_for=idle_for,
        )


def create_server(address: str, pool: futures.ThreadPoolExecutor) -> grpc.Server:
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


# Exit Codes for particular errors
EC_OK = 0
EC_TASK_DID_NOT_TERMINATE = 16


def run_agent(
    max_concurrent_rpcs: int,
    listening_address: str,
    last_task_timeout: float = 5.0,
) -> int:
    """Run the agent servicer on the given address."""
    # The pool has to be larger than the maximum number of concurrent
    # RPCs to ensure that the task processing thread is executed all
    # the time.
    with futures.ThreadPoolExecutor(max_workers=max_concurrent_rpcs + 1) as pool:
        server = grpc.server(
            pool,
            maximum_concurrent_rpcs=max_concurrent_rpcs,
            options=get_default_options(),
        )

        # Local server credentials allow us to ensure that the
        # connection is established by a local process.
        server_credentials = grpc.local_server_credentials()
        server.add_secure_port(listening_address, server_credentials)

        # Create the servicer and the task processing thread.
        servicer = AgentV2Servicer()
        done_event = Event()
        task_processing_task = pool.submit(
            servicer.process_tasks,
            done_event,
        )

        server.start()
        try:
            server.wait_for_termination()
        finally:
            done_event.set()
            try:
                task_processing_task.result(timeout=last_task_timeout)
            except TimeoutError:
                _log("The task processing thread did not terminate in time.")
                return EC_TASK_DID_NOT_TERMINATE
        return EC_OK


def main() -> int:
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        "address",
        default="localhost:50051",
        help="The address to listen on.",
    )
    parser.add_argument(
        "--max-concurrent-rpcs",
        type=int,
        default=1,
        help="The maximum number of concurrent RPCs to handle.",
    )
    parser.add_argument(
        "--last-task-timeout",
        type=float,
        default=5.0,
        help="The maximum time to wait for the last task to finish.",
    )
    args = parser.parse_args()

    return run_agent(
        args.max_concurrent_rpcs,
        args.address,
        args.last_task_timeout,
    )


if __name__ == "__main__":
    main()
