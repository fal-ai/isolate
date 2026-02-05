from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from functools import total_ordering
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from isolate.backends import BaseEnvironment

_SYSTEM_TEMP_DIR = Path(tempfile.gettempdir())


class LogSource(str, Enum):
    """Represents where the log orinates from."""

    # During the environment creation process (e.g. if the environment
    # is already created/cached, then no logs from this source will be
    # emitted).
    BUILDER = "builder"

    # During the environment execution process (from the server<->agent
    # communication, mostly for debugging purposes).
    BRIDGE = "bridge"

    # From the user script itself (e.g. a print() call in the given
    # function). The stream will be attached as level (stdout or stderr)
    USER = "user"


@total_ordering
class LogLevel(Enum):
    """Represents the log level."""

    TRACE = 0
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40

    # For user scripts
    STDOUT = 100
    STDERR = 110

    def __lt__(self, other: LogLevel) -> bool:
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented

    def __str__(self) -> str:
        return self.name.lower()


@dataclass
class Log:
    """A structured log message with an option source and level."""

    message: str
    source: LogSource
    level: LogLevel = LogLevel.INFO
    bound_env: BaseEnvironment | None = field(default=None, repr=False)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    is_json: bool = field(default=False)
    _parsed_message: dict | None = field(default=None, init=False, repr=False)

    def __str__(self) -> str:
        parts = [self.timestamp.strftime("%m/%d/%Y %H:%M:%S")]
        if self.bound_env:
            parts.append(f"[{self.bound_env.key[:6]}]")
        else:
            parts.append("[global]")

        parts.append(f"[{self.source}]".ljust(10))
        parts.append(f"[{self.level}]".ljust(10))
        return " ".join(parts) + self.message

    def message_str(self) -> str:
        parsed = self.from_json()
        return parsed["line"] if "line" in parsed else self.message

    def message_meta(self) -> dict:
        parsed = self.from_json()
        if "line" in parsed:
            # The metadata is everything except the actual log line.
            return {k: v for k, v in parsed.items() if k != "line"}
        return parsed

    def from_json(self) -> dict[str, str]:
        if not self.is_json:
            return {}
        if self._parsed_message is not None:
            return self._parsed_message
        try:
            self._parsed_message = json.loads(self.message)
            return self._parsed_message
        except json.JSONDecodeError:
            self._parsed_message = {}
            return self._parsed_message
