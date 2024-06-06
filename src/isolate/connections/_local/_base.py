from __future__ import annotations

import os
import subprocess
import sysconfig
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Iterator,
    TypeVar,
)

from isolate import __version__ as isolate_version
from isolate.backends.common import get_executable_path, logged_io
from isolate.connections.common import AGENT_SIGNATURE
from isolate.logs import LogLevel, LogSource

if TYPE_CHECKING:
    from isolate.backends import BaseEnvironment

ConnectionType = TypeVar("ConnectionType")


def binary_path_for(*search_paths: Path) -> str:
    """Return the binary search path for the given 'search_paths'.
    It will be a combination of the 'bin/' folders in them and
    the existing PATH environment variable."""

    paths = []
    for search_path in search_paths:
        path = sysconfig.get_path("scripts", vars={"base": search_path})
        paths.append(path)
        # Some distributions (conda) might include both a 'bin' and
        # a 'scripts' folder.

        auxilary_binary_path = search_path / "bin"
        if path != auxilary_binary_path and auxilary_binary_path.exists():
            paths.append(str(auxilary_binary_path))

    if "PATH" in os.environ:
        paths.append(os.environ["PATH"])

    return os.pathsep.join(paths)


def python_path_for(*search_paths: Path) -> str:
    """Return the PYTHONPATH for the library paths residing
    in the given 'search_paths'. The order of the paths is
    preserved."""
    assert len(search_paths) >= 1
    lib_paths = []
    for search_path in search_paths:
        # sysconfig defines the schema of the directories under
        # any comforming Python installation (like venv, conda, etc.).
        #
        # Be aware that Debian's system installation does not
        # comform sysconfig.
        raw_glob_expr = sysconfig.get_path(
            "purelib",
            vars={
                "base": search_path,
                "python_version": "*",
                "py_version_short": "*",
                "py_version_nodot": "*",
            },
        )
        relative_glob_expr = Path(raw_glob_expr).relative_to(search_path).as_posix()

        # Try to find expand the Python version in the path. This is
        # necessary for supporting multiple Python versions in the same
        # environment.
        for file in search_path.glob(relative_glob_expr):
            lib_paths.append(str(file))

    return os.pathsep.join(lib_paths)


@dataclass
class PythonExecutionBase(Generic[ConnectionType]):
    """A generic Python execution implementation that can trigger a new process
    and start watching stdout/stderr for the logs. The environment_path must be
    the base directory of a new Python environment (structure that complies with
    sysconfig). Python binary inside that environment will be used to run the
    agent process.

    If set, extra_inheritance_paths allows extending the custom package search
    system with additional environments. As an example, the current environment_path
    might point to an environment with numpy and the extra_inheritance_paths might
    point to an environment with pandas. In this case, the agent process will have
    access to both numpy and pandas. The order is important here, as the first
    path in the list will be the first one to be looked up (so if there is multiple
    versions of the same package in different environments, the one in the first
    path will take precedence). Dependency resolution and compatibility must be
    handled by the user."""

    environment: BaseEnvironment
    environment_path: Path
    extra_inheritance_paths: list[Path] = field(default_factory=list)

    @contextmanager
    def start_process(
        self,
        connection: ConnectionType,
        *args: Any,
        **kwargs: Any,
    ) -> Iterator[subprocess.Popen]:
        """Start the agent process with the Python binary available inside the
        bound environment."""

        python_executable = get_executable_path(self.environment_path, "python")
        with logged_io(
            partial(
                self.handle_agent_log, source=LogSource.USER, level=LogLevel.STDOUT
            ),
            partial(
                self.handle_agent_log, source=LogSource.USER, level=LogLevel.STDERR
            ),
            partial(
                self.handle_agent_log, source=LogSource.BRIDGE, level=LogLevel.TRACE
            ),
        ) as (stdout, stderr, log_fd):
            yield subprocess.Popen(
                self.get_python_cmd(python_executable, connection, log_fd),
                env=self.get_env_vars(),
                stdout=stdout,
                stderr=stderr,
                pass_fds=(log_fd,),
                text=True,
            )

    def get_env_vars(self) -> dict[str, str]:
        """Return the environment variables to run the agent process with. By default
        PYTHONUNBUFFERED is set to 1 to ensure the prints to stdout/stderr are reflect
        immediately (so that we can seamlessly transfer logs)."""

        custom_vars = {}
        custom_vars["ISOLATE_SERVER_VERSION"] = isolate_version
        custom_vars[AGENT_SIGNATURE] = "1"
        custom_vars["PYTHONUNBUFFERED"] = "1"

        # NOTE: we don't have to manually set PYTHONPATH here if we are
        # using a single environment since python will automatically
        # use the proper path.
        if self.extra_inheritance_paths:
            # The order here should reflect the order of the inheritance
            # where the actual environment already takes precedence.
            custom_vars["PYTHONPATH"] = python_path_for(
                self.environment_path, *self.extra_inheritance_paths
            )

        # But the PATH must be always set since it will be not be
        # automatically set by Python (think of this as ./venv/bin/activate)
        custom_vars["PATH"] = binary_path_for(
            self.environment_path, *self.extra_inheritance_paths
        )

        return {
            **os.environ,
            **custom_vars,
        }

    def get_python_cmd(
        self,
        executable: Path,
        connection: ConnectionType,
        log_fd: int,
    ) -> list[str | Path]:
        """Return the command to run the agent process with."""
        raise NotImplementedError

    def handle_agent_log(self, line: str, level: LogLevel, source: LogSource) -> None:
        """Handle a log line emitted by the agent process. The level will be either
        STDOUT or STDERR."""
        raise NotImplementedError
