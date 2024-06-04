from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

from isolate.backends import BaseEnvironment
from isolate.backends.common import sha256_digest_of
from isolate.backends.settings import DEFAULT_SETTINGS, IsolateSettings
from isolate.connections import PythonIPC


@dataclass
class ContainerizedPythonEnvironment(BaseEnvironment[Path]):
    BACKEND_NAME: ClassVar[str] = "container"

    image: dict[str, Any] = field(default_factory=dict)
    python_version: str | None = None
    requirements: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)

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
        # dockerfile_str is always there, but the validation is handled by the
        # controller.
        dockerfile_str = self.image.get("dockerfile_str", "")
        return sha256_digest_of(dockerfile_str, *self.requirements, *sorted(self.tags))

    def create(self, *, force: bool = False) -> Path:
        return Path(sys.exec_prefix)

    def destroy(self, connection_key: Path) -> None:
        raise NotImplementedError("ContainerizedPythonEnvironment cannot be destroyed")

    def exists(self) -> bool:
        return True

    def open_connection(self, connection_key: Path) -> PythonIPC:
        return PythonIPC(self, connection_key)
