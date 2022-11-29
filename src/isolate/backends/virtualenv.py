from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Union

from isolate.backends import BaseEnvironment, EnvironmentCreationError
from isolate.backends.common import (
    active_python,
    get_executable_path,
    logged_io,
    sha256_digest_of,
)
from isolate.backends.settings import DEFAULT_SETTINGS, IsolateSettings
from isolate.connections import PythonIPC


@dataclass
class VirtualPythonEnvironment(BaseEnvironment[Path]):
    BACKEND_NAME: ClassVar[str] = "virtualenv"

    requirements: List[str] = field(default_factory=list)
    constraints_file: Optional[os.PathLike] = None
    python_version: Optional[str] = None
    extra_index_urls: List[str] = field(default_factory=list)

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
        if self.constraints_file is not None:
            with open(self.constraints_file) as stream:
                constraints = stream.read().splitlines()
        else:
            constraints = []

        active_python_version = self.python_version or active_python()
        return sha256_digest_of(
            active_python_version,
            *self.requirements,
            *constraints,
            *self.extra_index_urls,
        )

    def install_requirements(self, path: Path) -> None:
        """Install the requirements of this environment using 'pip' to the
        given virtualenv path.

        If there are any constraint files specified, they will be also passed to
        the package resolver.
        """
        if not self.requirements:
            return None

        self.log(f"Installing requirements: {', '.join(self.requirements)}")

        pip_cmd: List[Union[str, os.PathLike]] = [
            get_executable_path(path, "pip"),
            "install",
            *self.requirements,
        ]
        if self.constraints_file:
            pip_cmd.extend(["-c", self.constraints_file])

        for extra_index_url in self.extra_index_urls:
            pip_cmd.extend(["--extra-index-url", extra_index_url])

        with logged_io(self.log) as (stdout, stderr):
            try:
                subprocess.check_call(
                    pip_cmd,
                    stdout=stdout,
                    stderr=stderr,
                )
            except subprocess.SubprocessError as exc:
                raise EnvironmentCreationError("Failure during 'pip install'.") from exc

    def _install_python_through_pyenv(self) -> str:
        from isolate.backends.pyenv import PyenvEnvironment

        self.log(
            f"Requested Python version of {self.python_version} is not available "
            "in the system, attempting to install it through pyenv."
        )

        pyenv = PyenvEnvironment.from_config(
            {"python_version": self.python_version},
            settings=self.settings,
        )
        return str(get_executable_path(pyenv.create(), "python"))

    def _decide_python(self) -> str:
        from virtualenv.discovery import builtin

        from isolate.backends.pyenv import _get_pyenv_executable

        interpreter = builtin.get_interpreter(self.python_version, ())
        if interpreter is not None:
            return interpreter.executable

        try:
            _get_pyenv_executable()
        except Exception:
            raise EnvironmentCreationError(
                f"Python {self.python_version} is not available in your "
                "system. Please install it first."
            ) from None
        else:
            return self._install_python_through_pyenv()

    def create(self) -> Path:
        from virtualenv import cli_run

        venv_path = self.settings.cache_dir_for(self)
        with self.settings.cache_lock_for(venv_path):
            if venv_path.exists():
                return venv_path

            self.log(f"Creating the environment at '{venv_path}'")

            args = [str(venv_path)]
            if self.python_version:
                args.append(f"--python={self._decide_python()}")

            try:
                cli_run(args)
            except (RuntimeError, OSError) as exc:
                raise EnvironmentCreationError(
                    f"Failed to create the environment at '{venv_path}'"
                ) from exc

            self.install_requirements(venv_path)

        self.log(f"New environment cached at '{venv_path}'")
        return venv_path

    def destroy(self, connection_key: Path) -> None:
        with self.settings.cache_lock_for(connection_key):
            # It might be destroyed already (when we are awaiting
            # for the lock to be released).
            if not connection_key.exists():
                return

            shutil.rmtree(connection_key)

    def exists(self) -> bool:
        path = self.settings.cache_dir_for(self)
        return path.exists()

    def open_connection(self, connection_key: Path) -> PythonIPC:
        return PythonIPC(self, connection_key)
