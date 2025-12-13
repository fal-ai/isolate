from __future__ import annotations

import os
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Iterator

from platformdirs import user_cache_dir

from isolate.backends.common import lock_build_path
from isolate.logs import Log, LogLevel, LogSource

if TYPE_CHECKING:
    from isolate.backends import BaseEnvironment

_SYSTEM_TEMP_DIR = Path(tempfile.gettempdir())
_STRICT_CACHE = os.getenv("ISOLATE_STRICT_CACHE", "0") == "1"
JSON_LOGS = os.getenv("ISOLATE_JSON_LOGS", "false") == "true"


@dataclass(frozen=True)
class IsolateSettings:
    cache_dir: Path = Path(user_cache_dir("isolate", "isolate"))
    serialization_method: str = "pickle"
    log_hook: Callable[[Log], None] = print
    strict_cache: bool = _STRICT_CACHE
    json_logs: bool = JSON_LOGS

    def log(self, log: Log) -> None:
        self.log_hook(self._infer_log_level(log))

    def _infer_log_level(self, log: Log) -> Log:
        """Infer the log level if it's correctly set."""
        if log.level not in (LogLevel.STDOUT, LogLevel.STDERR):
            # We should only infer the log level for stdout/stderr logs.
            return log

        if log.source in (LogSource.BUILDER, LogSource.BRIDGE):
            return replace(log, level=LogLevel.TRACE)

        line = log.message_str().lower()

        if line.startswith("error") or "[error]" in line:
            return replace(log, level=LogLevel.ERROR)
        if line.startswith("warning") or "[warning]" in line:
            return replace(log, level=LogLevel.WARNING)
        if line.startswith("warn") or "[warn]" in line:
            return replace(log, level=LogLevel.WARNING)
        if line.startswith("info") or "[info]" in line:
            return replace(log, level=LogLevel.INFO)
        if line.startswith("debug") or "[debug]" in line:
            return replace(log, level=LogLevel.DEBUG)
        if line.startswith("trace") or "[trace]" in line:
            return replace(log, level=LogLevel.TRACE)

        # Default all to INFO level, even STDERR
        return replace(log, level=LogLevel.INFO)

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

    def _get_lock_dir(self) -> Path:
        """Return a directory which can be used for storing file-based locks."""
        lock_dir = self._get_temp_base() / "locks"
        lock_dir.mkdir(exist_ok=True, parents=True)
        return lock_dir

    @contextmanager
    def cache_lock_for(self, path: Path) -> Iterator[Path]:
        """Create a lock for accessing (and operating on) the given path. This
        means whenever the context manager is entered, the path can be freely
        modified and accessed without any other process interfering."""

        with lock_build_path(path, self._get_lock_dir()):
            try:
                yield path
            except BaseException:
                # If anything goes wrong, we have to clean up the
                # directory (we can't leave it as a corrupted build).
                shutil.rmtree(path, ignore_errors=True)
                raise

    def cache_dir_for(self, backend: BaseEnvironment) -> Path:
        """Return a directory which can be used for caching the given
        environment's artifacts."""
        backend_name = backend.BACKEND_NAME
        assert backend_name is not None

        environment_base_path = self.cache_dir / backend_name
        environment_base_path.mkdir(exist_ok=True, parents=True)
        return environment_base_path / backend.key

    def completion_marker_for(self, path: Path) -> Path:
        return path / ".isolate.completed"

    replace = replace


DEFAULT_SETTINGS = IsolateSettings()
