from __future__ import annotations

import functools
import importlib
import os
import shutil
import sysconfig
import threading
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterator, Optional, Tuple

if TYPE_CHECKING:
    from isolate.backends._base import BaseEnvironment


@contextmanager
def rmdir_on_fail(path: Path) -> Iterator[None]:
    """Recursively remove the 'path; directory if there
    were any exceptions raised while this context is active."""

    try:
        yield
    except Exception:
        if path.exists():
            shutil.rmtree(path)
        raise


def python_path_for(*search_paths: Path) -> str:
    """Return the PYTHONPATH for the library paths residing
    in the given 'search_paths'. The order of the paths is
    preserved."""

    assert len(search_paths) >= 1
    return os.pathsep.join(
        # sysconfig defines the schema of the directories under
        # any comforming Python installation (like venv, conda, etc.).
        #
        # Be aware that Debian's system installation does not
        # comform sysconfig.
        sysconfig.get_path("purelib", vars={"base": search_path})
        for search_path in search_paths
    )


def get_executable_path(search_path: Path, executable_name: str) -> Path:
    """Return the path for the executable named 'executable_name' under
    the '/bin' directory of 'search_path'."""

    bin_dir = (search_path / "bin").as_posix()
    executable_path = shutil.which(executable_name, path=bin_dir)
    if executable_path is None:
        raise FileNotFoundError(
            f"Could not find '{executable_name}' in '{search_path}'. "
            f"Is the virtual environment corrupted?"
        )

    return Path(executable_path)


_NO_HIT = object()


def cache_static(func):
    """Cache the result of an parameter-less function."""
    _function_cache = _NO_HIT

    @functools.wraps(func)
    def wrapper():
        nonlocal _function_cache
        if _function_cache is _NO_HIT:
            _function_cache = func()
        return _function_cache

    return wrapper


_MESSAGE_STREAM_DELAY = 0.1


def _observe_reader(
    reading_fd: int,
    termination_event: threading.Event,
    hook: Callable[[str], None],
) -> threading.Thread:
    """Starts a new thread that reads from the specified file descriptor
    ('reading_fd') and calls the 'hook' for each line until the EOF is
    reached.

    Caller is responsible for joining the thread.
    """

    assert not os.get_blocking(reading_fd), "reading_fd must be non-blocking"

    def _reader():
        with open(reading_fd) as stream:
            while not termination_event.wait(_MESSAGE_STREAM_DELAY):
                line = stream.readline()
                if not line:
                    continue

                # TODO: probably only strip the last newline, or
                # do not strip anything at all.
                hook(line.rstrip())

            # Once the termination is requested, read everything
            # that is left in the stream.
            for line in stream.readlines():
                hook(line.rstrip())

    observer_thread = threading.Thread(target=_reader)
    observer_thread.start()
    return observer_thread


def _unblocked_pipe() -> Tuple[int, int]:
    """Create a pair of unblocked pipes. This is actually
    the same as os.pipe2(os.O_NONBLOCK), but that is not
    available in MacOS so we have to do it manually."""

    read_fd, write_fd = os.pipe()
    os.set_blocking(read_fd, False)
    os.set_blocking(write_fd, False)
    return read_fd, write_fd


@contextmanager
def logged_io(
    stdout_hook: Callable[[str], None],
    stderr_hook: Optional[Callable[[str], None]] = None,
) -> Iterator[Tuple[int, int]]:
    """Open two new streams (for stdout and stderr, respectively) and start relaying all
    the output from them to the given hooks."""

    termination_event = threading.Event()

    stdout_reader_fd, stdout_writer_fd = _unblocked_pipe()
    stderr_reader_fd, stderr_writer_fd = _unblocked_pipe()

    stdout_observer = _observe_reader(
        stdout_reader_fd,
        termination_event,
        hook=stdout_hook,
    )
    stderr_observer = _observe_reader(
        stderr_reader_fd,
        termination_event,
        hook=stderr_hook or stdout_hook,
    )
    try:
        yield stdout_writer_fd, stderr_writer_fd
    finally:
        termination_event.set()
        try:
            stdout_observer.join(timeout=_MESSAGE_STREAM_DELAY)
            stderr_observer.join(timeout=_MESSAGE_STREAM_DELAY)
        except TimeoutError:
            raise RuntimeError("Log observers did not terminate in time.")


def run_serialized(serialization_backend_name: str, data: bytes) -> Any:
    """Deserialize the given 'data' into an parameter-less callable and
    run it."""

    serialization_backend = importlib.import_module(serialization_backend_name)
    executable = serialization_backend.loads(data)
    return executable()
