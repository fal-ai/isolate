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
    Dict,
    Generic,
    Iterator,
    List,
    TypeVar,
    Union,
)

from isolate.backends.common import get_executable_path, logged_io
from isolate.logs import LogLevel

if TYPE_CHECKING:
    from isolate.backends import BaseEnvironment

ConnectionType = TypeVar("ConnectionType")


def python_path_for(*search_paths: Path) -> str:
    """Return the PYTHONPATH for the library paths residing
    in the given 'search_paths'. The order of the paths is
    preserved."""

    assert len(search_paths) >= 1
    return os.pathsep.join(
        # sysconfig defines the schema of the directories under
        # any comforming Python installation (like venv, conda, etc.).
        #
        # Be aware that Debian's system installation does not
        # comform sysconfig.
        sysconfig.get_path("purelib", vars={"base": search_path})
        for search_path in search_paths
    )


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
    extra_inheritance_paths: List[Path] = field(default_factory=list)

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
            partial(self.handle_agent_log, level=LogLevel.STDOUT),
            partial(self.handle_agent_log, level=LogLevel.STDERR),
        ) as (stdout, stderr):
            yield subprocess.Popen(
                self.get_python_cmd(python_executable, connection),
                env=self.get_env_vars(),
                stdout=stdout,
                stderr=stderr,
                text=True,
            )

    def get_env_vars(self) -> Dict[str, str]:
        """Return the environment variables to run the agent process with. By default
        PYTHONUNBUFFERED is set to 1 to ensure the prints to stdout/stderr are reflect
        immediately (so that we can seamlessly transfer logs)."""

        custom_vars = {}
        custom_vars["PYTHONUNBUFFERED"] = "1"
        if self.extra_inheritance_paths:
            # The order here should reflect the order of the inheritance
            # where the actual environment already takes precedence.
            python_path = python_path_for(
                self.environment_path, *self.extra_inheritance_paths
            )
            custom_vars["PYTHONPATH"] = python_path

        return {
            **os.environ,
            **custom_vars,
        }

    def get_python_cmd(
        self,
        executable: Path,
        connection: ConnectionType,
    ) -> List[Union[str, Path]]:
        """Return the command to run the agent process with."""
        raise NotImplementedError

    def handle_agent_log(self, line: str, level: LogLevel) -> None:
        """Handle a log line emitted by the agent process. The level will be either
        STDOUT or STDERR."""
        raise NotImplementedError
