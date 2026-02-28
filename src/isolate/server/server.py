from __future__ import annotations

import functools
import os
import signal
import threading
import time
import traceback
import uuid
from argparse import ArgumentParser
from collections import defaultdict
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field, replace
from queue import Empty as QueueEmpty
from queue import Queue
from typing import Any, Callable, Iterator, cast

import grpc
from grpc import ServicerContext, StatusCode
from grpc.experimental import wrap_server_method_handler

from isolate.backends import (
    EnvironmentCreationError,
    IsolateSettings,
)
from isolate.backends.common import Requirements, active_python
from isolate.backends.local import LocalPythonEnvironment
from isolate.backends.virtualenv import VirtualPythonEnvironment
from isolate.connections.grpc import AgentError, LocalPythonGRPC
from isolate.connections.grpc.configuration import get_default_options
from isolate.logger import IsolateLogger
from isolate.logs import Log, LogLevel, LogSource
from isolate.server import definitions, health
from isolate.server.health_server import HealthServicer
from isolate.server.interface import from_grpc, to_grpc

EMPTY_MESSAGE_INTERVAL = float(os.getenv("ISOLATE_EMPTY_MESSAGE_INTERVAL", "600"))
SKIP_EMPTY_LOGS = os.getenv("ISOLATE_SKIP_EMPTY_LOGS") == "1"
MAX_GRPC_WAIT_TIMEOUT = float(os.getenv("ISOLATE_MAX_GRPC_WAIT_TIMEOUT", "10.0"))

# Whether to inherit all the packages from the current environment or not.
INHERIT_FROM_LOCAL = os.getenv("ISOLATE_INHERIT_FROM_LOCAL") == "1"

# Number of threads that the gRPC server will use.
MAX_THREADS = int(os.getenv("MAX_THREADS", "5"))
_AGENT_REQUIREMENTS_TXT = os.getenv("AGENT_REQUIREMENTS_TXT")

if _AGENT_REQUIREMENTS_TXT is not None:
    with open(_AGENT_REQUIREMENTS_TXT) as stream:
        AGENT_REQUIREMENTS = stream.read().splitlines()
else:
    AGENT_REQUIREMENTS = []


# Number of seconds to observe the queue before checking the termination
# event.
_Q_WAIT_DELAY = 0.1


class GRPCException(Exception):
    def __init__(self, message: str, code: StatusCode = StatusCode.INVALID_ARGUMENT):
        super().__init__(message)
        self.message = message
        self.code = code

    def __str__(self) -> str:
        return f"{self.code.name}: {self.message}"


@dataclass
class RunnerAgent:
    stub: definitions.AgentStub
    message_queue: Queue[definitions.PartialRunResult]
    _bound_context: ExitStack
    _channel_state_history: list[grpc.ChannelConnectivity] = field(default_factory=list)
    _connection: LocalPythonGRPC | None = None
    _terminated: bool = False

    def __post_init__(self):
        def switch_state(connectivity_update: grpc.ChannelConnectivity) -> None:
            self._channel_state_history.append(connectivity_update)

        self.channel.subscribe(switch_state)

    @property
    def channel(self) -> grpc.Channel:
        return self.stub._channel  # type: ignore

    @property
    def is_accessible(self) -> bool:
        try:
            last_known_state = self._channel_state_history[-1]
        except IndexError:
            last_known_state = None

        return last_known_state is grpc.ChannelConnectivity.READY

    def check_connectivity(self) -> bool:
        # Check whether the server is ready.
        # TODO: This is more of a hack rather than a guaranteed health check,
        # we might have to introduce the proper protocol to the agents as well
        # to make sure that they are ready to receive requests.
        return self.is_accessible

    def terminate(self) -> None:
        """
        Abort the agent first, then close the bound context.

        Closing the ExitStack tears down the gRPC channel; doing that before
        terminating the agent triggers an asyncio.CancelledError mid-request and
        the agent never receives SIGTERM. By aborting first we deliver SIGTERM
        while the connection is still alive, then close it.
        """
        self._terminated = True
        if self._connection:
            self._connection.abort_agent()
        self._bound_context.close()


@dataclass
class BridgeManager:
    _agent_access_lock: threading.Lock = field(default_factory=threading.Lock)
    _agents: dict[tuple[Any, ...], list[RunnerAgent]] = field(
        default_factory=lambda: defaultdict(list)
    )
    _stack: ExitStack = field(default_factory=ExitStack)

    @contextmanager
    def establish(
        self,
        connection: LocalPythonGRPC,
        queue: Queue,
    ) -> Iterator[RunnerAgent]:
        agent = self._allocate_new_agent(connection, queue)

        try:
            yield agent
        finally:
            self._cache_agent(connection, agent)

    def _cache_agent(
        self,
        connection: LocalPythonGRPC,
        agent: RunnerAgent,
    ) -> None:
        with self._agent_access_lock:
            self._agents[self._identify(connection)].append(agent)

    def _allocate_new_agent(
        self,
        connection: LocalPythonGRPC,
        queue: Queue,
    ) -> RunnerAgent:
        with self._agent_access_lock:
            available_agents = self._agents[self._identify(connection)]
            while available_agents:
                agent = available_agents.pop()
                if agent.check_connectivity():
                    return agent
                else:
                    agent.terminate()

        bound_context = ExitStack()
        stub = bound_context.enter_context(
            connection._establish_bridge(max_wait_timeout=MAX_GRPC_WAIT_TIMEOUT)
        )
        return RunnerAgent(stub, queue, bound_context, [], connection)

    def _identify(self, connection: LocalPythonGRPC) -> tuple[Any, ...]:
        return (
            connection.environment_path,
            *connection.extra_inheritance_paths,
        )

    def __enter__(self) -> BridgeManager:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        for agents in self._agents.values():
            for agent in agents:
                agent.terminate()

    def abort_unreachable_agents(self) -> None:
        for agents in self._agents.values():
            for agent in agents:
                connection = agent._connection
                if connection is not None and not connection.is_alive():
                    connection.abort_agent()
                    # maybe restart the agent?


@dataclass
class RunTask:
    request: definitions.BoundFunction
    future: futures.Future | None = None
    agent: RunnerAgent | None = None
    logger: IsolateLogger = field(default_factory=IsolateLogger.from_env)

    def cancel(self):
        while True:
            # Cancelling a running future is not possible, and it sometimes blocks,
            # which means we never terminate the agent. So check if it's not running
            if self.future and not self.future.running():
                self.future.cancel()

            if self.agent:
                self.agent.terminate()

            try:
                if self.future:
                    self.future.exception(timeout=0.1)
                return
            except futures.TimeoutError:
                pass

    @property
    def stream_logs(self) -> bool:
        return self.request.stream_logs


@dataclass
class IsolateServicer(definitions.IsolateServicer):
    bridge_manager: BridgeManager
    default_settings: IsolateSettings = field(default_factory=IsolateSettings)
    background_tasks: dict[str, RunTask] = field(default_factory=dict)

    task_message_queues: dict[str, Queue[definitions.PartialRunResult]] = field(
        default_factory=dict
    )

    _active_subscriptions: dict[str, threading.Event] = field(default_factory=dict)
    _subscription_lock: threading.Lock = field(default_factory=threading.Lock)

    _shutting_down: bool = field(default=False)

    _thread_pool: futures.ThreadPoolExecutor = field(
        default_factory=lambda: futures.ThreadPoolExecutor(max_workers=MAX_THREADS)
    )

    def _run_task(self, task: RunTask) -> Iterator[definitions.PartialRunResult]:
        messages: Queue[definitions.PartialRunResult] = Queue()
        environments = []
        for env in task.request.environments:
            try:
                environments.append((env.force, from_grpc(env)))
            except ValueError:
                raise GRPCException(f"Unknown environment kind: {env.kind}")
            except TypeError as exc:
                raise GRPCException(f"Invalid environment: {str(exc)}")

        if not environments:
            raise GRPCException(
                "At least one environment must be specified for a run!",
                StatusCode.INVALID_ARGUMENT,
            )

        log_handler = LogHandler(messages, task=task)

        run_settings = replace(
            self.default_settings,
            log_hook=log_handler.handle,
            serialization_method=task.request.function.method,
        )

        for _, environment in environments:
            environment.apply_settings(run_settings)

        _, primary_environment = environments[0]

        if AGENT_REQUIREMENTS:
            python_version = getattr(
                primary_environment, "python_version", active_python()
            )
            agent_environ = VirtualPythonEnvironment(
                requirements=Requirements.from_raw(AGENT_REQUIREMENTS),
                python_version=python_version,
            )
            agent_environ.apply_settings(run_settings)
            environments.insert(1, (False, agent_environ))

        extra_inheritance_paths = []
        if INHERIT_FROM_LOCAL:
            local_environment = LocalPythonEnvironment()
            extra_inheritance_paths.append(local_environment.create())

        with ThreadPoolExecutor(max_workers=1) as local_pool:
            environment_paths = []
            for should_force_create, environment in environments:
                future = local_pool.submit(
                    environment.create, force=should_force_create
                )
                yield from self.watch_queue_until_completed(messages, future.done)
                try:
                    # Assuming that the iterator above only stops yielding once
                    # the future is completed, the timeout here should be redundant
                    # but it is just in case.
                    environment_paths.append(future.result(timeout=0.1))
                except EnvironmentCreationError as e:
                    raise GRPCException(f"{e}", StatusCode.INVALID_ARGUMENT)

            primary_path, *inheritance_paths = environment_paths
            inheritance_paths.extend(extra_inheritance_paths)
            _, primary_environment = environments[0]

            connection = LocalPythonGRPC(
                primary_environment,
                primary_path,
                extra_inheritance_paths=inheritance_paths,
            )

            with self.bridge_manager.establish(connection, queue=messages) as agent:
                task.agent = agent
                function_call = definitions.FunctionCall(
                    function=task.request.function,
                    setup_func=task.request.setup_func,
                )
                if not task.request.HasField("setup_func"):
                    function_call.ClearField("setup_func")

                future = local_pool.submit(
                    _proxy_to_queue,
                    # The agent may have been cached, so use the agent's message queue
                    queue=agent.message_queue,
                    bridge=agent.stub,
                    input=function_call,
                )

                # Unlike above; we are not interested in the result value of future
                # here, since it will be already transferred to other side without
                # us even seeing (through the queue).
                yield from self.watch_queue_until_completed(
                    agent.message_queue, future.done
                )

                # But we still have to check whether there were any errors raised
                # during the execution, and handle them accordingly.
                exception = future.exception(timeout=0.1)
                if exception is not None:
                    # If this is an RPC error, propagate it as is without any
                    # further processing.

                    if isinstance(exception, grpc.RpcError):
                        # on abort, we terminate the process before we close the channel
                        # because we need to populate SIGTERM to the agent process
                        if (
                            agent._terminated
                            and exception.code() == StatusCode.UNAVAILABLE
                        ):
                            return
                        raise GRPCException(
                            str(exception),
                            exception.code(),
                        )

                    # Otherwise this is a bug in the agent itself, so needs
                    # to be propagated with more details.
                    for line in traceback.format_exception(
                        type(exception), exception, exception.__traceback__
                    ):
                        yield from self.log(line, level=LogLevel.ERROR)
                    if isinstance(exception, AgentError):
                        raise GRPCException(str(exception), StatusCode.ABORTED)
                    else:
                        raise GRPCException(
                            f"An unexpected error occurred: {exception}.",
                            StatusCode.UNKNOWN,
                        )

    def _run_task_in_background(self, task: RunTask, task_id: str) -> None:
        self.task_message_queues[task_id] = Queue()
        try:
            for message in self._run_task(task):
                self.task_message_queues[task_id].put_nowait(message)
            print(f"Task {task_id} finished with result: {message}")
        except Exception as e:
            print(f"Task {task_id} finished with error: {e}")
            raise e
        finally:
            self.background_tasks.pop(task_id, None)

    def Submit(
        self,
        request: definitions.SubmitRequest,
        context: ServicerContext,
    ) -> definitions.SubmitResponse:
        task = RunTask(request=request.function)
        self.set_metadata(task, request.metadata)

        task_id = str(uuid.uuid4())
        task.future = self._thread_pool.submit(
            self._run_task_in_background, task, task_id
        )

        print(f"Submitted a task {task_id}")

        self.background_tasks[task_id] = task

        return definitions.SubmitResponse(task_id=task_id)

    def Subscribe(
        self,
        request: definitions.SubscribeRequest,
        context: ServicerContext,
    ) -> Iterator[definitions.PartialRunResult]:
        task_id = request.task_id
        if (
            task_id not in self.background_tasks
            or task_id not in self.task_message_queues
        ):
            raise GRPCException(
                f"Task {task_id} not found.",
                StatusCode.NOT_FOUND,
            )

        future = self.background_tasks[task_id].future

        start_time = time.monotonic()
        while future is None and time.monotonic() - start_time < 10:
            time.sleep(0.1)
            future = self.background_tasks[task_id].future

        if future is None:
            raise GRPCException(
                f"Timeout waiting for task {task_id} to start.",
                StatusCode.DEADLINE_EXCEEDED,
            )

        # Cancel any existing subscriber for this task and register ourselves.
        cancelled = threading.Event()
        with self._subscription_lock:
            old_event = self._active_subscriptions.get(task_id)
            if old_event is not None:
                old_event.set()  # Signal the previous subscriber to stop
            self._active_subscriptions[task_id] = cancelled

        queue = self.task_message_queues[task_id]

        try:
            yield from self.watch_queue_until_completed(
                queue, future.done, cancelled=cancelled
            )

            if cancelled.is_set():
                raise GRPCException(
                    "Subscription to task "
                    f"{task_id} was superseded by a new subscriber.",
                    StatusCode.ABORTED,
                )

            exception = future.exception(timeout=0.1)
            if exception is not None:
                raise exception
        finally:
            with self._subscription_lock:
                # Only clean up if we are still the active subscriber
                if self._active_subscriptions.get(task_id) is cancelled:
                    del self._active_subscriptions[task_id]

    def SetMetadata(
        self,
        request: definitions.SetMetadataRequest,
        context: ServicerContext,
    ) -> definitions.SetMetadataResponse:
        if request.task_id not in self.background_tasks:
            raise GRPCException(
                f"Task {request.task_id} not found.",
                StatusCode.NOT_FOUND,
            )

        self.set_metadata(self.background_tasks[request.task_id], request.metadata)

        return definitions.SetMetadataResponse()

    def set_metadata(self, task: RunTask, metadata: definitions.TaskMetadata) -> None:
        task.logger.extra_labels = dict(metadata.logger_labels)

    def Run(
        self,
        request: definitions.BoundFunction,
        context: ServicerContext,
    ) -> Iterator[definitions.PartialRunResult]:
        try:
            task = RunTask(request=request)

            # HACK: we can support only one task at a time
            # TODO: move away from this when we use submit for env-aware tasks
            self.background_tasks["RUN"] = task
            yield from self._run_task(task)
        except GRPCException as exc:
            return self.abort_with_msg(
                exc.message,
                context,
                code=exc.code,
            )
        finally:
            self.background_tasks.pop("RUN", None)

    def List(
        self,
        request: definitions.ListRequest,
        context: ServicerContext,
    ) -> definitions.ListResponse:
        return definitions.ListResponse(
            tasks=[
                definitions.TaskInfo(task_id=task_id)
                for task_id in self.background_tasks.keys()
            ]
        )

    def Cancel(
        self,
        request: definitions.CancelRequest,
        context: ServicerContext,
    ) -> definitions.CancelResponse:
        task_id = request.task_id

        print(f"Canceling task {task_id}")
        task = self.background_tasks.get(task_id)
        if task is not None:
            task.cancel()

        return definitions.CancelResponse()

    def shutdown(self) -> None:
        if self._shutting_down:
            print("Shutdown already in progress...")
            return

        self._shutting_down = True
        task_count = len(self.background_tasks)
        print(f"Shutting down, canceling {task_count} tasks...")
        self.cancel_tasks()
        print("All tasks canceled.")

    def watch_queue_until_completed(
        self,
        queue: Queue,
        is_completed: Callable[[], bool],
        cancelled: threading.Event | None = None,
    ) -> Iterator[definitions.PartialRunResult]:
        """Watch the given queue until the is_completed function returns True.
        Note that even if the function is completed, this function might not
        finish until the queue is empty.

        If a ``cancelled`` event is provided and becomes set, the watcher
        will stop yielding immediately so the caller can handle the
        cancellation (e.g. a subscription superseded by a new subscriber).
        """

        timer = time.monotonic()
        while not is_completed():
            if cancelled is not None and cancelled.is_set():
                return
            try:
                yield queue.get(timeout=_Q_WAIT_DELAY)
            except QueueEmpty:
                # Send an empty (but 'real') packet to the client, currently a hacky way
                # to make sure the stream results are never ignored.
                if time.monotonic() - timer > EMPTY_MESSAGE_INTERVAL:
                    timer = time.monotonic()
                    yield definitions.PartialRunResult(
                        is_complete=False,
                        logs=[],
                        result=None,
                    )

        # Clear the final messages
        while not queue.empty() and not (cancelled is not None and cancelled.is_set()):
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

    def cancel_tasks(self):
        tasks_copy = self.background_tasks.copy()
        for task in tasks_copy.values():
            task.cancel()


def _proxy_to_queue(
    queue: Queue,
    bridge: definitions.AgentStub,
    input: definitions.FunctionCall,
) -> None:
    for message in bridge.Run(input):
        queue.put_nowait(message)


@dataclass
class LogHandler:
    messages: Queue
    # Reference to the task so we can change the logger
    task: RunTask

    def handle(self, log: Log) -> None:
        if not SKIP_EMPTY_LOGS or log.message_str().strip():
            self.task.logger.log(
                log.level,
                log.message_str(),
                source=log.source,
                line_labels=log.message_meta(),
            )
            self._add_log_to_queue(log)

    def _add_log_to_queue(self, log: Log) -> None:
        if not self.task.stream_logs:
            # We do not queue the logs if the stream_logs is disabled
            # but still log them to the logger.
            return

        grpc_log = cast(definitions.Log, to_grpc(log))
        grpc_result = definitions.PartialRunResult(
            is_complete=False,
            logs=[grpc_log],
            result=None,
        )
        self.messages.put_nowait(grpc_result)


@dataclass
class ServerBoundInterceptor(grpc.ServerInterceptor):
    _server: grpc.Server | None = None
    _servicer: IsolateServicer | None = None

    def register_server(self, server: grpc.Server) -> None:
        if self._server is not None:
            raise RuntimeError("A server is already bound to this interceptor.")

        self._server = server

    @property
    def server(self) -> grpc.Server:
        if self._server is None:
            raise RuntimeError("No server was bound to this interceptor.")

        return self._server

    def register_servicer(self, servicer: IsolateServicer) -> None:
        if self._servicer is not None:
            raise RuntimeError("A servicer is already bound to this interceptor.")

        self._servicer = servicer

    @property
    def servicer(self) -> IsolateServicer:
        if self._servicer is None:
            raise RuntimeError("No servicer was bound to this interceptor.")

        return self._servicer


class SingleTaskInterceptor(ServerBoundInterceptor):
    """Sets server to terminate after the first Submit/Run task."""

    _done: bool = False
    _task_id: str | None = None

    def __init__(self):
        def terminate(request: Any, context: grpc.ServicerContext) -> Any:
            context.abort(
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                "Server has already served one Run/Submit task.",
            )

        self._terminator = grpc.unary_unary_rpc_method_handler(terminate)

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)

        is_submit = handler_call_details.method == "/Isolate/Submit"
        is_run = handler_call_details.method == "/Isolate/Run"
        is_new_task = is_submit or is_run

        if not is_new_task:
            # Let other requests like List/Cancel/etc pass through
            return handler

        if self._done:
            # Fail the request if the server has already served or is serving
            # a Run/Submit task.
            return self._terminator

        self._done = True

        def wrapper(method_impl):
            @functools.wraps(method_impl)
            def _wrapper(request: Any, context: grpc.ServicerContext) -> Any:
                def termination() -> None:
                    if is_run:
                        print("Stopping server since run is finished")
                        self.servicer.shutdown()
                        # Stop the server after the Run task is finished
                        self.server.stop(grace=0.1)
                        print("Server stopped")

                    elif is_submit:
                        # Wait until the task_id is assigned
                        while self._task_id is None:
                            time.sleep(0.1)

                        # Get the task from the background tasks
                        task = self.servicer.background_tasks.get(self._task_id)

                        if task is not None:
                            # Wait until the task future is assigned
                            tries = 0
                            while task.future is None:
                                time.sleep(0.1)
                                tries += 1
                                if tries > 100:
                                    raise RuntimeError(
                                        "Task future was not assigned in time."
                                    )

                            def _stop(*args):
                                # Small sleep to make sure the cancellation is processed
                                time.sleep(0.3)
                                print("Stopping server since the task is finished")
                                self.servicer.shutdown()
                                self.server.stop(grace=0.1)
                                print("Server stopped")

                            # Add a callback which will stop the server
                            # after the task is finished
                            task.future.add_done_callback(_stop)

                context.add_callback(termination)
                res = method_impl(request, context)

                if is_submit:
                    self._task_id = cast(definitions.SubmitResponse, res).task_id

                return res

            return _wrapper

        return wrap_server_method_handler(wrapper, handler)


class ControllerAuthInterceptor(ServerBoundInterceptor):
    def __init__(self, controller_auth_key: str) -> None:
        super().__init__()
        self.controller_auth_key = controller_auth_key
        self._terminator = grpc.unary_unary_rpc_method_handler(
            lambda request, context: context.abort(
                grpc.StatusCode.UNAUTHENTICATED, "Unauthorized"
            )
        )

    def intercept_service(self, continuation, handler_call_details):
        metadata = dict(handler_call_details.invocation_metadata)
        controller_token = metadata.get("controller-token")
        if controller_token != self.controller_auth_key:
            return self._terminator

        return continuation(handler_call_details)


def main(argv: list[str] | None = None) -> None:
    parser = ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=50001)
    parser.add_argument(
        "--single-use",
        action="store_true",
        help="Terminate the server after the first Run or Submit task is completed.",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=MAX_THREADS,
        help="Number of worker threads to use for the gRPC server.",
    )

    options = parser.parse_args(argv)
    if options.num_workers is None:
        options.num_workers = 1 if options.single_use else os.cpu_count()

    interceptors: list[ServerBoundInterceptor] = []
    if options.single_use:
        interceptors.append(SingleTaskInterceptor())

    controller_auth_key = os.getenv("ISOLATE_CONTROLLER_AUTH_KEY")
    if not controller_auth_key:
        # DEPRECATED: remove this after rolling new version of controller
        controller_auth_key = os.getenv("CONTROLLER_KEY")

    if controller_auth_key:
        # Set an interceptor to only accept requests with the correct auth key
        interceptors.append(ControllerAuthInterceptor(controller_auth_key))
    else:
        print(
            "[WARN] ISOLATE_CONTROLLER_AUTH_KEY is not set, all requests will be "
            "accepted without authentication."
        )

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=options.num_workers),
        options=get_default_options(),
        interceptors=interceptors,  # type: ignore
    )

    for interceptor in interceptors:
        interceptor.register_server(server)

    with BridgeManager() as bridge_manager:
        servicer = IsolateServicer(bridge_manager)

        for interceptor in interceptors:
            interceptor.register_servicer(servicer)

        definitions.register_isolate(servicer, server)
        health.register_health(HealthServicer(), server)

        def handle_termination(*args):
            print("Termination signal received, shutting down...")
            servicer.shutdown()
            server.stop(grace=0.1)

        def handle_child_termination(*args):
            print("Child termination signal received, aborting unreachable agents...")
            bridge_manager.abort_unreachable_agents()

        signal.signal(signal.SIGINT, handle_termination)
        signal.signal(signal.SIGTERM, handle_termination)
        signal.signal(signal.SIGCHLD, handle_child_termination)

        server.add_insecure_port(f"[::]:{options.port}")
        print(f"Started listening at {options.host}:{options.port}")

        server.start()
        server.wait_for_termination()
        print("Server shut down")


if __name__ == "__main__":
    main()
