from __future__ import annotations

import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from enum import Enum
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


class LogLevel(str, Enum):
    """Represents the log level."""

    TRACE = "trace"
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"

    # For user scripts
    STDOUT = "stdout"
    STDERR = "stderr"


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
