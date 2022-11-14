from __future__ import annotations

import hashlib
import os
import secrets
import shutil
import threading
import time
from contextlib import contextmanager, suppress
from functools import lru_cache
from pathlib import Path
from typing import Callable, Iterator, Optional, Tuple

_OLD_DIR_PREFIX = "old-"

# For ensuring that the lock is created and not forgotten
# (e.g. the process which acquires it crashes, so it is never
# released), we are going to check the lock file's mtime every
# _REVOKE_LOCK_DELAY seconds. If the mtime is older than that
# value, we are going to assume the lock is stale and revoke it.
_REVOKE_LOCK_DELAY = 30


def replace_dir(src_path: Path, dst_path: Path, lock_dir: Path) -> None:
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

    tmp_path = None
    try:
        with _lock_file_for(dst_path, lock_dir):
            if dst_path.exists():
                # Rename the destination directory to a temporary location.
                # This is to ensure that the destination directory is not
                # removed before we can rename the source directory to it.
                tmp_path = dst_path.parent / (
                    _OLD_DIR_PREFIX + secrets.token_hex(16) + dst_path.name
                )
                dst_path.rename(tmp_path)

            src_path.rename(dst_path)
    finally:
        # No matter what happens, we need to remove the temporary
        # directory.
        if tmp_path is not None:
            shutil.rmtree(tmp_path, ignore_errors=True)


@contextmanager
def _lock_file_for(path: Path, lock_dir: Path) -> Iterator[None]:
    """Try to acquire a lock for all operations on the given 'path'."""
    lock_file = (lock_dir / path.name).with_suffix(".lock")
    try:
        while not _try_acquire(lock_file):
            time.sleep(0.05)
            continue
        yield
    finally:
        with suppress(FileNotFoundError):
            lock_file.unlink()


def _try_acquire(lock_file: Path) -> bool:
    with suppress(FileNotFoundError):
        mtime = lock_file.stat().st_mtime
        if time.time() - mtime > _REVOKE_LOCK_DELAY:
            # The lock file exists, but it may be stale. Check the
            # mtime and if it is too old, revoke it.
            lock_file.unlink()

    try:
        lock_file.touch(exist_ok=False)
    except FileExistsError:
        return False
    else:
        return True


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
