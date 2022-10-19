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
    temp_path_for,
)
from isolate.backends.connections import PythonIPC
from isolate.backends.context import GLOBAL_CONTEXT, ContextType


@dataclass
class VirtualPythonEnvironment(BaseEnvironment[Path]):
    BACKEND_NAME: ClassVar[str] = "virtualenv"

    requirements: List[str]
    constraints_file: Optional[os.PathLike] = None

    @classmethod
    def from_config(
        cls,
        config: Dict[str, Any],
        context: ContextType = GLOBAL_CONTEXT,
    ) -> BaseEnvironment:
        requirements = config.get("requirements", [])
        # TODO: we probably should validate that this file actually exists
        constraints_file = config.get("constraints_file", None)
        environment = cls(
            requirements=requirements,
            constraints_file=constraints_file,
        )
        environment.set_context(context)
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

        cache_dir = self.context.get_cache_dir(self)
        cache_dir.mkdir(parents=True, exist_ok=True)

        cache_path = cache_dir / self.key
        if cache_path.exists():
            return cache_path

        with temp_path_for(cache_path) as path:
            self.log(f"Creating the environment at '{path}'")

            try:
                cli_run([str(path)])
            except OSError as exc:
                raise EnvironmentCreationError(
                    f"Failed to create the environment at '{path}'"
                ) from exc

            self.install_requirements(path)

        assert cache_path.exists()
        self.log(f"New environment cached at '{cache_path}'")
        return cache_path

    def destroy(self, connection_key: Path) -> None:
        shutil.rmtree(connection_key)

    def exists(self) -> bool:
        path = self.context.get_cache_dir(self) / self.key
        return path.exists()

    def open_connection(self, connection_key: Path) -> PythonIPC:
        return PythonIPC(self, connection_key)
