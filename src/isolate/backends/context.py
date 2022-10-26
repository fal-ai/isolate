from __future__ import annotations

import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, Iterator, NewType, Optional

from platformdirs import user_cache_dir

from isolate.backends.common import replace_dir

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

    def serialize(self) -> Dict[str, str]:
        return {
            "message": self.message,
            "source": format(self.source),
            "level": format(self.level),
        }

    def __str__(self) -> str:
        parts = []
        if self.bound_env:
            parts.append(f"[{self.bound_env.key[:6]}]")
        else:
            parts.append("[global]")

        parts.append(f"[{self.source}]".ljust(10))
        parts.append(f"[{self.level}]".ljust(10))
        return " ".join(parts) + self.message


@dataclass(frozen=True)
class IsolateSettings:
    cache_dir: Path = Path(user_cache_dir("isolate", "isolate"))
    serialization_method: str = "pickle"
    log_hook: Callable[[Log], None] = print

    def log(self, log: Log) -> None:
        self.log_hook(log)

    def _get_temp_base(self) -> Path:
        """Return the base path for creating temporary files/directories.

        If the isolate cache directory is in a different device than the
        system temp base (e.g. /tmp), then it will return a new directory
        under the cache directory."""

        cache_stat = self.cache_dir.stat()
        system_stat = _SYSTEM_TEMP_DIR.stat()
        if cache_stat.st_dev == system_stat.st_dev:
            return _SYSTEM_TEMP_DIR

        if _SYSTEM_TEMP_DIR.samefile(self.cache_dir):
            path = _SYSTEM_TEMP_DIR / "isolate"
        else:
            # This is quite important since if we have a shared cache
            # disk, then /tmp is going to be in a different disk than
            # the cache directory, which would make it impossible to
            # rename() atomically.
            path = self.cache_dir / "tmp"

        path.mkdir(exist_ok=True, parents=True)
        return path

    @contextmanager
    def build_ctx_for(self, dst_path: Path) -> Iterator[Path]:
        """Create a new build context for the given 'dst_path'. It will return
        a temporary directory which can be used to create the environment and
        once the context is closed, the build directory will be moved to
        the destination."""

        tmp_path = Path(tempfile.mkdtemp(dir=self._get_temp_base()))
        try:
            yield tmp_path
        except BaseException as exc:
            shutil.rmtree(tmp_path)
            raise exc
        else:
            replace_dir(tmp_path, dst_path)

    def cache_dir_for(self, backend: BaseEnvironment) -> Path:
        """Return a directory which can be used for caching the given
        environment's artifacts."""
        backend_name = backend.BACKEND_NAME
        assert backend_name is not None

        environment_base_path = self.cache_dir / backend_name
        environment_base_path.mkdir(exist_ok=True, parents=True)
        return environment_base_path / backend.key


DEFAULT_SETTINGS = IsolateSettings()
