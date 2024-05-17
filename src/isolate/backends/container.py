from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

from isolate.backends import BaseEnvironment
from isolate.backends.settings import DEFAULT_SETTINGS, IsolateSettings
from isolate.connections import PythonIPC


@dataclass
class ContainerizedPythonEnvironment(BaseEnvironment[Path]):
    BACKEND_NAME: ClassVar[str] = "container"

    image: str

    @classmethod
    def from_config(
        cls,
        config: dict[str, Any],
        settings: IsolateSettings = DEFAULT_SETTINGS,
    ) -> BaseEnvironment:
        environment = cls(**config)
        environment.apply_settings(settings)
        return environment

    @property
    def key(self) -> str:
        return self.image

    def create(self, *, _force: bool = False) -> Path:
        return Path(sys.exec_prefix)

    def destroy(self, _connection_key: Path) -> None:
        raise NotImplementedError("ContainerizedPythonEnvironment cannot be destroyed")

    def exists(self) -> bool:
        return True

    def open_connection(self, connection_key: Path) -> PythonIPC:
        return PythonIPC(self, connection_key)
