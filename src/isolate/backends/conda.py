from __future__ import annotations

import functools
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional

from isolate.backends import BaseEnvironment, EnvironmentCreationError
from isolate.backends.common import active_python, logged_io, sha256_digest_of
from isolate.backends.settings import DEFAULT_SETTINGS, IsolateSettings
from isolate.connections import PythonIPC

# Specify the path where the conda binary might reside in (or
# mamba, if it is the preferred one).
_CONDA_COMMAND = os.environ.get("CONDA_EXE", "conda")
_ISOLATE_CONDA_HOME = os.getenv("ISOLATE_CONDA_HOME")

# Conda accepts the following version specifiers: =, ==, >=, <=, >, <, !=
_CONDA_VERSION_IDENTIFIER_CHARS = (
    "=",
    "<",
    ">",
    "!",
)


@dataclass
class CondaEnvironment(BaseEnvironment[Path]):
    BACKEND_NAME: ClassVar[str] = "conda"

    packages: List[str] = field(default_factory=list)
    python_version: Optional[str] = None

    @classmethod
    def from_config(
        cls,
        config: Dict[str, Any],
        settings: IsolateSettings = DEFAULT_SETTINGS,
    ) -> BaseEnvironment:
        environment = cls(**config)
        environment.apply_settings(settings)
        return environment

    @property
    def key(self) -> str:
        return sha256_digest_of(*self._compute_dependencies())

    def _compute_dependencies(self) -> List[str]:
        user_dependencies = self.packages.copy()
        for raw_requirement in user_dependencies:
            # Get rid of all whitespace characters (python = 3.8 becomes python=3.8)
            raw_requirement = raw_requirement.replace(" ", "")
            if not raw_requirement.startswith("python"):
                continue

            # Ensure that the package is either python or python followed
            # by a version specifier. Examples:
            #  - python # OK
            #  - python=3.8 # OK
            #  - python>=3.8 # OK
            #  - python-user-toolkit # NOT OK
            #  - pythonhelp!=1.0 # NOT OK

            python_suffix = raw_requirement[len("python") :]
            if (
                python_suffix
                and python_suffix[0] not in _CONDA_VERSION_IDENTIFIER_CHARS
            ):
                continue

            raise EnvironmentCreationError(
                "Python version can not be specified by packages (it needs to be passed as `python_version` option)"
            )

        # Now that we verified that the user did not specify the Python version
        # we can add it by ourselves
        target_python = self.python_version or active_python()
        user_dependencies.append(f"python={target_python}")
        return user_dependencies

    def create(self) -> Path:
        env_path = self.settings.cache_dir_for(self)
        with self.settings.cache_lock_for(env_path):
            if env_path.exists():
                return env_path

            # Since our agent needs Python to be installed (at very least)
            # we need to make sure that the base environment is created with
            # the same Python version as the one that is used to run the
            # isolate agent.
            dependencies = self._compute_dependencies()

            self.log(f"Creating the environment at '{env_path}'")
            self.log(f"Installing packages: {', '.join(dependencies)}")

            try:
                self._run_conda(
                    "create",
                    "--yes",
                    "--prefix",
                    env_path,
                    *dependencies,
                )
            except subprocess.SubprocessError as exc:
                raise EnvironmentCreationError("Failure during 'conda create'") from exc

        self.log(f"New environment cached at '{env_path}'")
        return env_path

    def destroy(self, connection_key: Path) -> None:
        with self.settings.cache_lock_for(connection_key):
            # It might be destroyed already (when we are awaiting
            # for the lock to be released).
            if not connection_key.exists():
                return

            self._run_conda(
                "remove",
                "--yes",
                "--all",
                "--prefix",
                connection_key,
            )

    def _run_conda(self, *args: Any) -> None:
        conda_executable = _get_conda_executable()
        with logged_io(self.log) as (stdout, stderr):
            subprocess.check_call(
                [conda_executable, *args],
                stdout=stdout,
                stderr=stderr,
            )

    def exists(self) -> bool:
        path = self.settings.cache_dir_for(self)
        return path.exists()

    def open_connection(self, connection_key: Path) -> PythonIPC:
        return PythonIPC(self, connection_key)


@functools.lru_cache(1)
def _get_conda_executable() -> Path:
    for path in [_ISOLATE_CONDA_HOME, None]:
        conda_path = shutil.which(_CONDA_COMMAND, path=path)
        if conda_path is not None:
            return Path(conda_path)
    else:
        # TODO: we should probably show some instructions on how you
        # can install conda here.
        raise FileNotFoundError(
            "Could not find conda executable. If conda executable is not available by default, please point isolate "
            " to the path where conda binary is available 'ISOLATE_CONDA_HOME'."
        )
