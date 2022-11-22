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

_PYENV_EXECUTABLE_NAME = "pyenv"
_PYENV_EXECUTABLE_PATH = os.environ.get("ISOLATE_PYENV_EXECUTABLE")


@dataclass
class PyenvEnvironment(BaseEnvironment[Path]):
    BACKEND_NAME: ClassVar[str] = "pyenv"

    python_version: str

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
        return os.path.join("versions", self.python_version)

    def create(self) -> Path:
        pyenv = _get_pyenv_executable()
        env_path = self.settings.cache_dir_for(self)
        with self.settings.cache_lock_for(env_path):
            # PyEnv installs* the Python versions under $root/versions/$version, where we
            # use versions/$version as the key and $root as the base directory (for pyenv).
            #
            # [0]: https://github.com/pyenv/pyenv#locating-pyenv-provided-python-installations
            pyenv_root = env_path.parent.parent
            prefix = self._try_get_prefix(pyenv, pyenv_root)
            if prefix is None:
                self._install_python(pyenv, pyenv_root)
                prefix = self._try_get_prefix(pyenv, pyenv_root)
                if not prefix:
                    raise EnvironmentCreationError(
                        f"Python {self.python_version} must have been installed by now."
                    )

            assert prefix is not None
            return prefix

    def _try_get_prefix(self, pyenv: Path, root_path: Path) -> Optional[Path]:
        try:
            prefix = subprocess.check_output(
                [pyenv, "prefix", self.python_version],
                env={**os.environ, "PYENV_ROOT": str(root_path)},
                text=True,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as exc:
            if "not installed" in exc.stderr:
                return None
            raise EnvironmentCreationError(
                f"Failed to get the prefix for Python {self.python_version}.\n"
                f"{exc.stdout}\n{exc.stderr}"
            )

        return Path(prefix.strip())

    def _install_python(self, pyenv: Path, root_path: Path) -> None:
        with logged_io(self.log) as (stdout, stderr):
            try:
                subprocess.check_call(
                    [pyenv, "install", "--skip-existing", self.python_version],
                    env={**os.environ, "PYENV_ROOT": str(root_path)},
                    stdout=stdout,
                    stderr=stderr,
                )
            except subprocess.CalledProcessError as exc:
                raise EnvironmentCreationError(
                    f"Failed to install Python {self.python_version} via pyenv.\n"
                )

    def destroy(self, connection_key: Path) -> None:
        pyenv = _get_pyenv_executable()
        with self.settings.cache_lock_for(connection_key):
            # It might be destroyed already (when we are awaiting
            # for the lock to be released).
            if not connection_key.exists():
                return None

            pyenv_root = connection_key.parent.parent
            with logged_io(self.log) as (stdout, stderr):
                subprocess.check_call(
                    [pyenv, "uninstall", "-f", connection_key.name],
                    env={**os.environ, "PYENV_ROOT": str(pyenv_root)},
                    stdout=stdout,
                    stderr=stderr,
                )

    def exists(self) -> bool:
        pyenv = _get_pyenv_executable()
        cache_dir = self.settings.cache_dir_for(self)
        with self.settings.cache_lock_for(cache_dir):
            pyenv_root = cache_dir.parent.parent
            prefix = self._try_get_prefix(pyenv, pyenv_root)
            return prefix is not None

    def open_connection(self, connection_key: Path) -> PythonIPC:
        return PythonIPC(self, connection_key)


@functools.lru_cache(1)
def _get_pyenv_executable() -> Path:
    if _PYENV_EXECUTABLE_PATH:
        if not os.path.exists(_PYENV_EXECUTABLE_PATH):
            raise EnvironmentCreationError(
                f"Given ISOLATE_PYENV_EXECUTABLE path does not exist in the system: {_PYENV_EXECUTABLE_PATH!r}"
            )
        return Path(_PYENV_EXECUTABLE_PATH)

    pyenv_path = shutil.which(_PYENV_EXECUTABLE_NAME)
    if pyenv_path is None:
        raise FileNotFoundError(
            "Could not find the pyenv executable. If pyenv is not already installed in your system, please"
            "install it first. If it is not in your PATH, then point ISOLATE_PYENV_COMMAND to the absolute path of the"
            "pyenv executable."
        )
    return Path(pyenv_path)