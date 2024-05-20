from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, ClassVar

from isolate.backends import BaseEnvironment, EnvironmentCreationError
from isolate.backends.common import (
    active_python,
    get_executable,
    get_executable_path,
    logged_io,
    optional_import,
    sha256_digest_of,
)
from isolate.backends.settings import DEFAULT_SETTINGS, IsolateSettings
from isolate.connections import PythonIPC
from isolate.logs import LogLevel

_UV_RESOLVER_EXECUTABLE = os.environ.get("ISOLATE_UV_EXE", "uv")
_UV_RESOLVER_HOME = os.getenv("ISOLATE_UV_HOME")


@dataclass
class VirtualPythonEnvironment(BaseEnvironment[Path]):
    BACKEND_NAME: ClassVar[str] = "virtualenv"

    requirements: list[str] = field(default_factory=list)
    constraints_file: os.PathLike | None = None
    python_version: str | None = None
    extra_index_urls: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    resolver: str | None = None

    @classmethod
    def from_config(
        cls,
        config: dict[str, Any],
        settings: IsolateSettings = DEFAULT_SETTINGS,
    ) -> BaseEnvironment:
        environment = cls(**config)
        environment.apply_settings(settings)
        if environment.resolver not in ("uv", None):
            raise ValueError(
                "Only 'uv' is supported as a resolver for virtualenv environments."
            )
        return environment

    @property
    def key(self) -> str:
        if self.constraints_file is not None:
            with open(self.constraints_file) as stream:
                constraints = stream.read().splitlines()
        else:
            constraints = []

        extras = []
        if self.resolver is not None:
            extras.append(f"resolver={self.resolver}")

        active_python_version = self.python_version or active_python()
        return sha256_digest_of(
            active_python_version,
            *self.requirements,
            *constraints,
            *self.extra_index_urls,
            *sorted(self.tags),
            # This is backwards compatible with environments not using
            # the 'resolver' field.
            *extras,
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
        environ = os.environ.copy()

        if self.resolver == "uv":
            # Set VIRTUAL_ENV to the actual path of the environment since that is
            # how uv discovers the environment. This is necessary when using uv
            # as the resolver.
            environ["VIRTUAL_ENV"] = str(path)
            base_pip_cmd = [
                get_executable(_UV_RESOLVER_EXECUTABLE, _UV_RESOLVER_HOME),
                "pip",
            ]
        else:
            base_pip_cmd = [get_executable_path(path, "pip")]

        pip_cmd: list[str | os.PathLike] = [
            *base_pip_cmd,  # type: ignore
            "install",
            *self.requirements,
        ]
        if self.constraints_file:
            pip_cmd.extend(["-c", self.constraints_file])

        for extra_index_url in self.extra_index_urls:
            pip_cmd.extend(["--extra-index-url", extra_index_url])

        with logged_io(partial(self.log, level=LogLevel.INFO)) as (stdout, stderr, _):
            try:
                subprocess.check_call(
                    pip_cmd,
                    stdout=stdout,
                    stderr=stderr,
                    env=environ,
                )
            except subprocess.SubprocessError as exc:
                raise EnvironmentCreationError(f"Failure during 'pip install': {exc}")

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
        from isolate.backends.pyenv import _get_pyenv_executable

        builtin_discovery = optional_import("virtualenv.discovery.builtin")
        interpreter = builtin_discovery.get_interpreter(self.python_version, ())
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

    def create(self, *, force: bool = False) -> Path:
        virtualenv = optional_import("virtualenv")

        venv_path = self.settings.cache_dir_for(self)
        completion_marker = self.settings.completion_marker_for(venv_path)
        with self.settings.cache_lock_for(venv_path):
            if not force:
                is_cached = venv_path.exists()
                if self.settings.strict_cache:
                    is_cached &= completion_marker.exists()

                if is_cached:
                    return venv_path

            self.log(f"Creating the environment at '{venv_path}'")

            args = [str(venv_path)]
            if self.python_version:
                args.append(f"--python={self._decide_python()}")

            try:
                virtualenv.cli_run(args)
            except (RuntimeError, OSError) as exc:
                raise EnvironmentCreationError(
                    f"Failed to create the environment at '{venv_path}': {exc}"
                )

            self.install_requirements(venv_path)
            completion_marker.touch()

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
