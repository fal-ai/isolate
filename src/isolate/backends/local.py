from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Dict

from isolate.backends import BaseEnvironment
from isolate.backends.common import sha256_digest_of
from isolate.backends.settings import DEFAULT_SETTINGS, IsolateSettings
from isolate.connections import PythonIPC


@dataclass
class LocalPythonEnvironment(BaseEnvironment[Path]):
    BACKEND_NAME: ClassVar[str] = "local"

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
        return sha256_digest_of(sys.exec_prefix)

    def create(self) -> Path:
        return Path(sys.exec_prefix)

    def destroy(self, connection_key: Path) -> None:
        raise NotImplementedError("LocalPythonEnvironment cannot be destroyed")

    def exists(self) -> bool:
        return True

    def open_connection(self, connection_key: Path) -> PythonIPC:
        return PythonIPC(self, connection_key)
