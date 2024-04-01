from __future__ import annotations

import os
import threading
import time
import traceback
from collections import defaultdict
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field, replace
from functools import partial
from queue import Empty as QueueEmpty
from queue import Queue
from typing import Any, Callable, Iterator, cast

import grpc
from grpc import ServicerContext, StatusCode

from isolate.backends import (
    EnvironmentCreationError,
    IsolateSettings,
)
from isolate.backends.common import active_python
from isolate.backends.local import LocalPythonEnvironment
from isolate.backends.virtualenv import VirtualPythonEnvironment
from isolate.connections.grpc import AgentError, LocalPythonGRPC
from isolate.connections.grpc.configuration import get_default_options
from isolate.logs import Log, LogLevel, LogSource
from isolate.server import definitions, health
from isolate.server.health_server import HealthServicer
from isolate.server.interface import from_grpc, to_grpc

EMPTY_MESSAGE_INTERVAL = float(os.getenv("ISOLATE_EMPTY_MESSAGE_INTERVAL", 600))
MAX_GRPC_WAIT_TIMEOUT = float(os.getenv("ISOLATE_MAX_GRPC_WAIT_TIMEOUT", 10.0))

# Whether to inherit all the packages from the current environment or not.
INHERIT_FROM_LOCAL = os.getenv("ISOLATE_INHERIT_FROM_LOCAL") == "1"

# Number of threads that the gRPC server will use.
MAX_THREADS = int(os.getenv("MAX_THREADS", 5))
_AGENT_REQUIREMENTS_TXT = os.getenv("AGENT_REQUIREMENTS_TXT")

if _AGENT_REQUIREMENTS_TXT is not None:
    with open(_AGENT_REQUIREMENTS_TXT) as stream:
        AGENT_REQUIREMENTS = stream.read().splitlines()
else:
    AGENT_REQUIREMENTS = []


# Number of seconds to observe the queue before checking the termination
# event.
_Q_WAIT_DELAY = 0.1


@dataclass
class RunnerAgent:
    stub: definitions.AgentStub
    message_queue: Queue
    _bound_context: ExitStack
    _channel_state_history: list[grpc.ChannelConnectivity] = field(default_factory=list)

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
    ) -> Iterator[tuple[definitions.AgentStub, Queue]]:
        agent = self._allocate_new_agent(connection, queue)

        try:
            yield agent.stub, agent.message_queue
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
        return RunnerAgent(stub, queue, bound_context)

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


@dataclass
class IsolateServicer(definitions.IsolateServicer):
    bridge_manager: BridgeManager
    default_settings: IsolateSettings = field(default_factory=IsolateSettings)

    def Run(
        self,
        request: definitions.BoundFunction,
        context: ServicerContext,
    ) -> Iterator[definitions.PartialRunResult]:
        messages: Queue[definitions.PartialRunResult] = Queue()
        environments = []
        for env in request.environments:
            try:
                environments.append((env.force, from_grpc(env)))
            except ValueError:
                return self.abort_with_msg(
                    f"Unknown environment kind: {env.kind}",
                    context,
                )
            except TypeError as exc:
                return self.abort_with_msg(
                    f"Invalid environment parameter: {str(exc)}.",
                    context,
                )

        if not environments:
            return self.abort_with_msg(
                "At least one environment must be specified for a run!",
                context,
            )

        run_settings = replace(
            self.default_settings,
            log_hook=partial(_add_log_to_queue, messages),
            serialization_method=request.function.method,
        )

        for _, environment in environments:
            environment.apply_settings(run_settings)

        _, primary_environment = environments[0]

        if AGENT_REQUIREMENTS:
            python_version = getattr(
                primary_environment, "python_version", active_python()
            )
            agent_environ = VirtualPythonEnvironment(
                requirements=AGENT_REQUIREMENTS,
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
                    return self.abort_with_msg(f"{e}", context)

            primary_path, *inheritance_paths = environment_paths
            inheritance_paths.extend(extra_inheritance_paths)
            _, primary_environment = environments[0]

            connection = LocalPythonGRPC(
                primary_environment,
                primary_path,
                extra_inheritance_paths=inheritance_paths,
            )

            with self.bridge_manager.establish(connection, queue=messages) as (
                bridge,
                queue,
            ):
                function_call = definitions.FunctionCall(
                    function=request.function,
                    setup_func=request.setup_func,
                )
                if not request.HasField("setup_func"):
                    function_call.ClearField("setup_func")

                future = local_pool.submit(
                    _proxy_to_queue,
                    queue=queue,
                    bridge=bridge,
                    input=function_call,
                )

                # Unlike above; we are not interested in the result value of future
                # here, since it will be already transferred to other side without
                # us even seeing (through the queue).
                yield from self.watch_queue_until_completed(queue, future.done)

                # But we still have to check whether there were any errors raised
                # during the execution, and handle them accordingly.
                exception = future.exception(timeout=0.1)
                if exception is not None:
                    # If this is an RPC error, propagate it as is without any
                    # further processing.
                    if isinstance(exception, grpc.RpcError):
                        return self.abort_with_msg(
                            exception.details(),
                            context,
                            code=exception.code(),
                        )

                    # Otherwise this is a bug in the agent itself, so needs
                    # to be propagated with more details.
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
                            f"An unexpected error occurred: {exception}.",
                            context,
                            code=StatusCode.UNKNOWN,
                        )

    def watch_queue_until_completed(
        self, queue: Queue, is_completed: Callable[[], bool]
    ) -> Iterator[definitions.PartialRunResult]:
        """Watch the given queue until the is_completed function returns True. Note that even
        if the function is completed, this function might not finish until the queue is empty.
        """

        timer = time.monotonic()
        while not is_completed():
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
    bridge: definitions.AgentStub,
    input: definitions.FunctionCall,
) -> None:
    for message in bridge.Run(input):
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
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=MAX_THREADS),
        options=get_default_options(),
    )
    with BridgeManager() as bridge_manager:
        definitions.register_isolate(IsolateServicer(bridge_manager), server)
        health.register_health(HealthServicer(), server)

        server.add_insecure_port("[::]:50001")
        print("Started listening at localhost:50001")

        server.start()
        server.wait_for_termination()


if __name__ == "__main__":
    main()
