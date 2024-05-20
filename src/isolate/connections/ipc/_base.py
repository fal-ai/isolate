from __future__ import annotations

import base64
import importlib
import subprocess
import time
from contextlib import ExitStack, closing
from dataclasses import dataclass
from multiprocessing.connection import Connection, Listener
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ContextManager,
)

from isolate.backends import (
    BasicCallable,
    CallResultType,
    EnvironmentConnection,
)
from isolate.connections._local import PythonExecutionBase, agent_startup
from isolate.connections.common import prepare_exc
from isolate.connections.ipc import agent
from isolate.logs import LogLevel, LogSource

if TYPE_CHECKING:
    # Somhow mypy can't figure out that `ConnectionWrapper`
    # really exists.
    class ConnectionWrapper(Connection):
        def __init__(
            self,
            connection: Any,
            loads: Callable[[bytes], Any],
            dumps: Callable[[Any], bytes],
        ) -> None: ...

        def recv(self) -> Any: ...

        def send(self, value: Any) -> None: ...

        def close(self) -> None: ...

else:
    from multiprocessing.connection import ConnectionWrapper


class AgentListener(Listener):
    """A custom listener that can use any available serialization method
    to communicate with the child process."""

    def __init__(self, backend_name: str, *args: Any, **kwargs: Any) -> None:
        self.serialization_backend = loadserialization_method(backend_name)
        super().__init__(*args, **kwargs)

    def accept(self) -> Connection:
        return ConnectionWrapper(
            super().accept(),
            dumps=self.serialization_backend.dumps,
            loads=self.serialization_backend.loads,
        )


def loadserialization_method(backend_name: str) -> Any:
    # TODO(feat): This should probably throw a better error if the
    # given backend does not exist.
    return importlib.import_module(backend_name)


def encode_service_address(address: tuple[str, int]) -> str:
    host, port = address
    return base64.b64encode(f"{host}:{port}".encode()).decode("utf-8")


@dataclass
class IsolatedProcessConnection(EnvironmentConnection):
    """A generic IPC implementation for running the isolate backend
    in a separated process.

    Each implementation needs to define a start_process method to
    spawn the agent."""

    # The amount of seconds to wait before checking whether the
    # isolated process has exited or not.
    _DEFER_THRESHOLD = 0.25

    def start_process(
        self,
        connection: AgentListener,
        *args: Any,
        **kwargs: Any,
    ) -> ContextManager[subprocess.Popen]:
        """Start the agent process."""
        raise NotImplementedError

    def run(  # type: ignore[return-value]
        self,
        executable: BasicCallable,
        *args: Any,
        **kwargs: Any,
    ) -> CallResultType:  # type: ignore[type-var]
        """Spawn an agent process using the given environment, run the given
        `executable` in that process, and return the result object back."""

        with ExitStack() as stack:
            # IPC flow is the following:
            #  1. [controller]: Create the socket server
            #  2. [controller]: Spawn the call agent with the socket address
            #  3.      [agent]: Connect to the socket server
            #  4. [controller]: Accept the incoming connection request
            #  5. [controller]: Send the executable over the established bridge
            #  6.      [agent]: Receive the executable from the bridge
            #  7.      [agent]: Execute the executable and once done send the result
            #                   back
            #  8. [controller]: Loop until either the isolated process exits or sends
            #                   any data (will be interpreted as a tuple of two
            #                   mutually exclusive objects, either a result object or
            #                   an exception to be raised).
            #

            self.log("Starting the controller bridge.")
            controller_service = stack.enter_context(
                AgentListener(
                    self.environment.settings.serialization_method,
                    family="AF_INET",
                )
            )

            self.log(
                f"Controller server is listening at {controller_service.address}."
                " Attempting to start the agent process."
            )
            assert not (args or kwargs), "run() should not receive any arguments."
            isolated_process = stack.enter_context(
                self.start_process(controller_service, *args, **kwargs)
            )

            # TODO(fix): this might hang if the agent process crashes before it can
            # connect to the controller bridge.
            self.log(
                f"Awaiting agent process of {isolated_process.pid}"
                " to establish a connection."
            )
            established_connection = stack.enter_context(
                closing(controller_service.accept())
            )

            self.log("Bridge between controller and the agent has been established.")
            established_connection.send(executable)

            self.log("Executable has been sent, awaiting execution result.")
            return self.poll_until_result(
                isolated_process,
                established_connection,
            )

    def poll_until_result(
        self,
        process: subprocess.Popen,
        connection: Connection,
    ) -> CallResultType:  # type: ignore[type-var]
        """Take the given process, and poll until either it exits or returns
        a result object."""

        while not connection.poll():
            # Normally, if we do connection.read() without having this loop
            # it is going to block us indefinitely (even if the underlying
            # process has crashed). We can use a combination of process.poll
            # and connection.poll to check if the process is alive and has data
            # to move forward.
            if process.poll():
                break

            # For preventing busy waiting, we can sleep for a bit
            # and let other threads run.
            time.sleep(self._DEFER_THRESHOLD)
            continue

        if not connection.poll():
            # If the process has exited but there is still no data, we
            # can assume something terrible has happened.
            raise OSError(
                "The isolated process has exited unexpectedly with code "
                f"'{process.poll()}' without sending any data back."
            )

        # TODO(fix): handle EOFError that might happen here (e.g. problematic
        # serialization might cause it).
        result, did_it_raise, stringized_traceback = connection.recv()

        if did_it_raise:
            raise prepare_exc(result, stringized_traceback=stringized_traceback)
        else:
            assert stringized_traceback is None
            return result


@dataclass
class PythonIPC(PythonExecutionBase[AgentListener], IsolatedProcessConnection):
    def get_python_cmd(
        self,
        executable: Path,
        connection: AgentListener,
        log_fd: int,
    ) -> list[str | Path]:
        assert isinstance(connection.address, tuple)
        return [
            executable,
            agent_startup.__file__,
            agent.__file__,
            encode_service_address(connection.address),
            # TODO(feat): we probably should check if the given backend is installed
            # on the remote interpreter, otherwise it will fail without establishing
            # the connection with the bridge.
            "--serialization-backend",
            self.environment.settings.serialization_method,
            "--log-fd",
            str(log_fd),
        ]

    def handle_agent_log(self, line: str, level: LogLevel, source: LogSource) -> None:
        self.log(line, level=level, source=source)
