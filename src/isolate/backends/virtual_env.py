from __future__ import annotations

import hashlib
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Dict, List

from isolate import BaseEnvironment
from isolate.common import BASE_CACHE_DIR, get_executable_path, rmdir_on_fail
from isolate.connections import PythonIPC

_BASE_VENV_DIR = BASE_CACHE_DIR / "venvs"
_BASE_VENV_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class VirtualPythonEnvironment(BaseEnvironment[Path]):
    BACKEND_NAME: ClassVar[str] = "virtualenv"

    requirements: List[str]

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> VirtualPythonEnvironment:
        requirements = config.get("requirements", [])
        return cls(requirements)

    @property
    def key(self) -> str:
        return hashlib.sha256(" ".join(self.requirements).encode()).hexdigest()

    def create(self, *, exist_ok: bool = False) -> Path:
        from virtualenv import cli_run

        path = self.context.get_cache_dir(self) / self.key
        if path.exists():
            if exist_ok:
                return path
            else:
                raise FileExistsError(f"Virtual environment already exists at '{path}'")

        with rmdir_on_fail(path):
            self.log("Creating the environment at '{}'", path, kind="info")
            cli_run([str(path)])

            if self.requirements:
                self.log(
                    "Installing requirements: {}",
                    ", ".join(self.requirements),
                    kind="info",
                )
                pip_path = get_executable_path(path, "pip")
                subprocess.check_call([str(pip_path), "install"] + self.requirements)

        return path

    def destroy(self, connection_key: Path) -> None:
        shutil.rmtree(connection_key)

    def open_connection(self, connection_key: Path) -> PythonIPC:
        return PythonIPC(self, connection_key)
