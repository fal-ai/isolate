import base64
import importlib
import os
import subprocess
import time
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass, field
from functools import partial
from multiprocessing.connection import ConnectionWrapper, Listener
from pathlib import Path
from typing import Any, ContextManager, Dict, Iterator, List, Tuple, Union

from insulate.backends import (
    BasicCallable,
    CallResultType,
    EnvironmentConnection,
    UserException,
)
from insulate.backends.common import (
    get_executable_path,
    logged_io,
    python_path_for,
)
from insulate.backends.connections.ipc import agent
from insulate.backends.context import LogLevel, LogSource


class _MultiFormatListener(Listener):
    def __init__(self, backend_name: str, *args: Any, **kwargs: Any) -> None:
        self.serialization_backend = load_serialization_backend(backend_name)
        super().__init__(*args, **kwargs)

    def accept(self) -> ConnectionWrapper:
        return closing(
            ConnectionWrapper(
                super().accept(),
                dumps=self.serialization_backend.dumps,
                loads=self.serialization_backend.loads,
            )
        )


def load_serialization_backend(backend_name: str) -> Any:
    # TODO(feat): This should probably throw a better error if the
    # given backend does not exist.
    return importlib.import_module(backend_name)


def encode_service_address(address: Tuple[str, int]) -> str:
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
        connection: ConnectionWrapper,
        *args: Any,
        **kwargs: Any,
    ) -> ContextManager[subprocess.Popen]:
        """Start the agent process."""
        raise NotImplementedError

    def run(
        self,
        executable: BasicCallable,
        ignore_exceptions: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> CallResultType:
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
            #  7.      [agent]: Execute the executable and once done send the result back
            #  8. [controller]: Loop until either the isolated process exits or sends any
            #                   data (will be interpreted as a tuple of two mutually exclusive
            #                   objects, either a result object or an exception to be raised).
            #

            self.log("Starting the controller bridge.")
            controller_service = stack.enter_context(
                _MultiFormatListener(
                    self.environment.context.serialization_backend_name,
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
            established_connection = stack.enter_context(controller_service.accept())

            self.log("Bridge between controller and the agent has been established.")
            established_connection.send(executable)

            self.log("Executable has been sent, awaiting execution result.")
            return self.poll_until_result(
                isolated_process,
                established_connection,
                ignore_exceptions,
            )

    def poll_until_result(
        self,
        process: subprocess.Popen,
        connection: ConnectionWrapper,
        ignore_exceptions: bool,
    ) -> CallResultType:
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
        result, did_it_raise = connection.recv()

        if did_it_raise:
            if ignore_exceptions:
                return UserException(result)  # type: ignore
            else:
                raise result
        else:
            return result


@dataclass
class PythonIPC(IsolatedProcessConnection):
    """A simply Python IPC implementation that takes the base directory
    of a new Python environment (structure that complies with sysconfig)
    and runs the agent process in that environment."""

    environment_path: Path

    @contextmanager
    def start_process(
        self,
        connection: ConnectionWrapper,
        *args: Any,
        **kwargs: Any,
    ) -> Iterator[subprocess.Popen]:
        """Start the Python agent process with using the Python interpreter from
        the given environment_path."""

        python_executable = get_executable_path(self.environment_path, "python")
        with logged_io(
            partial(self._parse_agent_and_log, level=LogLevel.STDOUT),
            partial(self._parse_agent_and_log, level=LogLevel.STDERR),
        ) as (stdout, stderr):
            yield subprocess.Popen(
                self._get_python_cmd(python_executable, connection),
                env=self._get_python_env(),
                stdout=stdout,
                stderr=stderr,
                text=True,
            )

    def _get_python_env(self):
        return {
            **os.environ,
            "PYTHONUNBUFFERED": "1",  # We want to stream the logs as they come.
        }

    def _get_python_cmd(
        self,
        executable: Path,
        connection: ConnectionWrapper,
    ) -> List[Union[str, Path]]:
        assert executable.exists(), f"Python executable {executable} does not exist."
        return [
            executable,
            agent.__file__,
            encode_service_address(connection.address),
            # TODO(feat): we probably should check if the given backend is installed
            # on the remote interpreter, otherwise it will fail without establishing
            # the connection with the bridge.
            "--serialization-backend",
            self.environment.context.serialization_backend_name,
        ]

    def _parse_agent_and_log(self, line: str, level: LogLevel) -> None:
        # TODO: we probably should create a new fd and pass it as
        # one of the the arguments to the child process. Then everything
        # from that fd can be automatically logged as originating from the
        # bridge.

        # Agent can produce [trace] messages, so change the log
        # level to it if this does not originate from the user.
        if line.startswith("[trace]"):
            line = line.replace("[trace]", "", 1)
            level = LogLevel.TRACE
            source = LogSource.BRIDGE
        else:
            source = LogSource.USER

        self.log(line, level=level, source=source)


# TODO: should we actually merge this with PythonIPC since it is
# simple enough and interchangeable?
@dataclass
class ExtendedPythonIPC(PythonIPC):
    """A Python IPC implementation that can also inherit packages from
    other environments (e.g. a virtual environment that has the core
    requirements like `dill` can be inherited on a new environment).

    The given extra_inheritance_paths should be a list of paths that
    comply with the sysconfig, and it should be ordered in terms of
    priority (e.g. the first path will be the most prioritized one,
    right after the current environment). So if two environments have
    conflicting versions of the same package, the first one present in
    the inheritance chain will be used.

    This works by including the `site-packages` directory of the
    inherited environment in the `PYTHONPATH` when starting the
    agent process.
    """

    extra_inheritance_paths: List[Path] = field(default_factory=list)

    def _get_python_env(self) -> Dict[str, str]:
        env_variables = super()._get_python_env()

        if self.extra_inheritance_paths:
            # The order here should reflect the order of the inheritance
            # where the actual environment already takes precedence.
            python_path = python_path_for(
                self.environment_path, *self.extra_inheritance_paths
            )
            env_variables["PYTHONPATH"] = python_path
        return env_variables
