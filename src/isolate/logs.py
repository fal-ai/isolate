from __future__ import annotations

import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from enum import Enum
from functools import total_ordering
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, Iterator, NewType, Optional

from platformdirs import user_cache_dir

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

    TRACE = "trace"
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"

    # For user scripts
    STDOUT = "stdout"
    STDERR = "stderr"

    @property
    def score(self) -> int:
        scores: dict[LogLevel, int] = {
            LogLevel.STDERR: 110,
            LogLevel.STDOUT: 100,
            LogLevel.ERROR: 40,
            LogLevel.WARNING: 30,
            LogLevel.INFO: 20,
            LogLevel.DEBUG: 10,
            LogLevel.TRACE: 0,
        }
        return scores[self]

    def __lt__(self, other: LogLevel) -> bool:
        if self.__class__ is other.__class__:
            return self.score < other.score
        return NotImplemented

    def __str__(self) -> str:
        return self.value

@dataclass
class Log:
    """A structured log message with an option source and level."""

    message: str
    source: LogSource
    level: LogLevel = LogLevel.INFO
    bound_env: Optional[BaseEnvironment] = field(default=None, repr=False)

    def __str__(self) -> str:
        parts = []
        if self.bound_env:
            parts.append(f"[{self.bound_env.key[:6]}]")
        else:
            parts.append("[global]")

        parts.append(f"[{self.source}]".ljust(10))
        parts.append(f"[{self.level}]".ljust(10))
        return " ".join(parts) + self.message
