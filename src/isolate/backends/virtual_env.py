from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

from isolate import BaseEnvironment
from isolate.common import BASE_CACHE_DIR, get_executable_path, rmdir_on_fail
from isolate.connections import PythonIPC

_BASE_VENV_DIR = BASE_CACHE_DIR / "venvs"
_BASE_VENV_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class VirtualPythonEnvironment(BaseEnvironment[Path]):
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

        path = _BASE_VENV_DIR / self.key
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

    def open_connection(self, conn_info: Path) -> PythonIPC:
        return PythonIPC(self, conn_info)
