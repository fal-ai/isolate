from __future__ import annotations

import hashlib
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Dict, List

from isolate.backends import BaseEnvironment, EnvironmentCreationError
from isolate.backends.common import (
    get_executable_path,
    logged_io,
    rmdir_on_fail,
)
from isolate.backends.connections import PythonIPC
from isolate.backends.context import GLOBAL_CONTEXT, ContextType


@dataclass
class VirtualPythonEnvironment(BaseEnvironment[Path]):
    BACKEND_NAME: ClassVar[str] = "virtualenv"

    requirements: List[str]

    @classmethod
    def from_config(
        cls,
        config: Dict[str, Any],
        context: ContextType = GLOBAL_CONTEXT,
    ) -> BaseEnvironment:
        requirements = config.get("requirements", [])
        environment = cls(requirements)
        environment.set_context(context)
        return environment

    @property
    def key(self) -> str:
        return hashlib.sha256(" ".join(self.requirements).encode()).hexdigest()

    def create(self) -> Path:
        from virtualenv import cli_run

        path = self.context.get_cache_dir(self) / self.key
        if path.exists():
            return path

        with rmdir_on_fail(path):
            self.log(f"Creating the environment at '{path}'")

            try:
                cli_run([str(path)])
            except OSError as exc:
                raise EnvironmentCreationError(
                    f"Failed to create the environment at '{path}'"
                ) from exc

            if self.requirements:
                self.log(f"Installing requirements: {', '.join(self.requirements)}")
                pip_path = get_executable_path(path, "pip")
                with logged_io(self.log) as (stdout, stderr):
                    try:
                        subprocess.check_call(
                            [str(pip_path), "install", *self.requirements],
                            stdout=stdout,
                            stderr=stderr,
                        )
                    except subprocess.SubprocessError as exc:
                        raise EnvironmentCreationError(
                            "Failure during 'pip install'."
                        ) from exc

        return path

    def destroy(self, connection_key: Path) -> None:
        shutil.rmtree(connection_key)

    def exists(self) -> bool:
        path = self.context.get_cache_dir(self) / self.key
        return path.exists()

    def open_connection(self, connection_key: Path) -> PythonIPC:
        return PythonIPC(self, connection_key)
