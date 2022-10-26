from __future__ import annotations

import hashlib
import os
import shutil
import threading
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import Callable, Iterator, Optional, Tuple

_OLD_DIR_PREFIX = "old-"


def replace_dir(src_path: Path, dst_path: Path) -> None:
    """Atomically replace 'dst_path' with 'src_path'.

    Be aware that this is not actually atomic (and there is no
    way to do so, at least in a cross-platform fashion). The basic
    idea is that, we first rename the 'dst_path' to something else
    (if it exists), and then rename the 'src_path' to 'dst_path' and
    finally remove the temporary directory in which we hold 'dst_path'.

    Prioritizing these two renames allows us to keep the cache always
    in a working state (if we were to remove 'dst_path' first and then
    try renaming 'src_path' to 'dst_path', any error while removing it
    would make the cache corrupted).
    """

    if dst_path.exists():
        cleanup = dst_path.rename(dst_path.with_name(_OLD_DIR_PREFIX + dst_path.name))
    else:
        cleanup = None

    assert not dst_path.exists()
    try:
        src_path.rename(dst_path)
    finally:
        if cleanup is not None:
            shutil.rmtree(cleanup)


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


@lru_cache(maxsize=None)
def sha256_digest_of(*unique_fields: str, _join_char: str = "\n") -> str:
    """Return the SHA256 digest that corresponds to the combined version
    of 'unique_fields. The order is preserved."""

    inner_text = _join_char.join(unique_fields).encode()
    return hashlib.sha256(inner_text).hexdigest()
