from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Union

from isolate.backends import BaseEnvironment, EnvironmentCreationError
from isolate.backends.common import (
    get_executable_path,
    logged_io,
    sha256_digest_of,
)
from isolate.backends.settings import DEFAULT_SETTINGS, IsolateSettings
from isolate.connections import PythonIPC


@dataclass
class VirtualPythonEnvironment(BaseEnvironment[Path]):
    BACKEND_NAME: ClassVar[str] = "virtualenv"

    requirements: List[str]
    constraints_file: Optional[os.PathLike] = None

    @classmethod
    def from_config(
        cls,
        config: Dict[str, Any],
        settings: IsolateSettings = DEFAULT_SETTINGS,
    ) -> BaseEnvironment:
        requirements = config.get("requirements", [])
        # TODO: we probably should validate that this file actually exists
        constraints_file = config.get("constraints_file", None)
        environment = cls(
            requirements=requirements,
            constraints_file=constraints_file,
        )
        environment.apply_settings(settings)
        return environment

    @property
    def key(self) -> str:
        if self.constraints_file is not None:
            with open(self.constraints_file) as stream:
                constraints = stream.read().splitlines()
        else:
            constraints = []
        return sha256_digest_of(*self.requirements, *constraints)

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

        with logged_io(self.log) as (stdout, stderr):
            try:
                subprocess.check_call(
                    pip_cmd,
                    stdout=stdout,
                    stderr=stderr,
                )
            except subprocess.SubprocessError as exc:
                raise EnvironmentCreationError("Failure during 'pip install'.") from exc

    def create(self) -> Path:
        from virtualenv import cli_run

        venv_path = self.settings.cache_dir_for(self)
        if venv_path.exists():
            return venv_path

        with self.settings.build_ctx_for(venv_path) as build_path:
            self.log(f"Creating the environment at '{build_path}'")

            try:
                cli_run([str(build_path)])
            except OSError as exc:
                raise EnvironmentCreationError(
                    f"Failed to create the environment at '{build_path}'"
                ) from exc

            self.install_requirements(build_path)

        assert venv_path.exists(), "Environment must be built at this point"
        self.log(f"New environment cached at '{venv_path}'")
        return venv_path

    def destroy(self, connection_key: Path) -> None:
        shutil.rmtree(connection_key)

    def exists(self) -> bool:
        path = self.settings.cache_dir_for(self)
        return path.exists()

    def open_connection(self, connection_key: Path) -> PythonIPC:
        return PythonIPC(self, connection_key)
