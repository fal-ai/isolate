from __future__ import annotations

import errno
import hashlib
import os
import select
import shutil
import sysconfig
import threading
import time
from contextlib import contextmanager, suppress
from functools import lru_cache
from pathlib import Path
from types import ModuleType
from typing import Callable, Iterator

# For ensuring that the lock is created and not forgotten
# (e.g. the process which acquires it crashes, so it is never
# released), we are going to check the lock file's mtime every
# _REVOKE_LOCK_DELAY seconds. If the mtime is older than that
# value, we are going to assume the lock is stale and revoke it.
_REVOKE_LOCK_DELAY = 30


@contextmanager
def lock_build_path(path: Path, lock_dir: Path) -> Iterator[None]:
    """Try to acquire a lock for all operations on the given 'path'. This guarantees
    that the path will not be modified by any other process while the lock is held."""
    lock_file = (lock_dir / path.name).with_suffix(".lock")
    while not _try_acquire(lock_file):
        time.sleep(0.05)
        continue

    with _keep_lock_alive(lock_file):
        yield


@contextmanager
def _keep_lock_alive(lock_file: Path) -> Iterator[None]:
    """Keep the lock file alive by updating its mtime as long
    as we are doing something in the cache."""
    event = threading.Event()

    def _keep_alive(per_beat_delay: float = 1) -> None:
        while not event.wait(per_beat_delay):
            lock_file.touch()
        lock_file.unlink()

    thread = threading.Thread(target=_keep_alive)
    try:
        thread.start()
        yield
    finally:
        event.set()
        thread.join()


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


_CHECK_FOR_TERMINATION_DELAY = 0.05
HookT = Callable[[str], None]


def _io_observer(
    hooks: dict[int, HookT],
    termination_event: threading.Event,
) -> threading.Thread:
    """Starts a new thread that reads from the specified file descriptors
    and calls the bound hook function for each line until the EOF is reached
    or the termination event is set.

    Caller is responsible for joining the thread.
    """

    followed_fds = list(hooks.keys())
    for fd in followed_fds:
        if os.get_blocking(fd):
            raise NotImplementedError(
                "All the hooked file descriptors must be non-blocking."
            )

    def forward_lines(fd: int) -> None:
        hook = hooks[fd]
        with open(fd, closefd=False) as stream:
            # TODO: we probably should pass the real line endings
            raw_data = stream.read()
            if not raw_data:
                return  # Nothing to read

            for line in raw_data.splitlines():
                hook(line)

    def _reader():
        while not termination_event.is_set():
            # The observed file descriptors may be closed by the
            # underlying process at any given time. So before we
            # make a select call, we need to check if the file
            # descriptors are still valid and remove the ones
            # that are not.
            for fd in followed_fds.copy():
                try:
                    os.fstat(fd)
                except OSError as exc:
                    if exc.errno == errno.EBADF:
                        followed_fds.remove(fd)

            if not followed_fds:
                # All the file descriptors are closed, so we can
                # stop the thread.
                return

            ready, _, _ = select.select(
                # rlist=
                followed_fds,
                # wlist=
                [],
                # xlist=
                [],
                # timeout=
                _CHECK_FOR_TERMINATION_DELAY,
            )
            for fd in ready:
                forward_lines(fd)

    observer_thread = threading.Thread(target=_reader)
    observer_thread.start()
    return observer_thread


def _unblocked_pipe() -> tuple[int, int]:
    """Create a pair of unblocked pipes. This is actually
    the same as os.pipe2(os.O_NONBLOCK), but that is not
    available in MacOS so we have to do it manually."""

    read_fd, write_fd = os.pipe()
    os.set_blocking(read_fd, False)
    os.set_blocking(write_fd, False)
    return read_fd, write_fd


@contextmanager
def logged_io(
    stdout_hook: HookT,
    stderr_hook: HookT | None = None,
    log_hook: HookT | None = None,
) -> Iterator[tuple[int, int, int]]:
    """Open two new streams (for stdout and stderr, respectively) and start relaying all
    the output from them to the given hooks."""

    stdout_reader_fd, stdout_writer_fd = _unblocked_pipe()
    stderr_reader_fd, stderr_writer_fd = _unblocked_pipe()
    log_reader_fd, log_writer_fd = _unblocked_pipe()

    termination_event = threading.Event()
    io_observer = _io_observer(
        hooks={
            stdout_reader_fd: stdout_hook,
            stderr_reader_fd: stderr_hook or stdout_hook,
            log_reader_fd: log_hook or stdout_hook,
        },
        termination_event=termination_event,
    )
    try:
        yield stdout_writer_fd, stderr_writer_fd, log_writer_fd
    finally:
        termination_event.set()
        try:
            # The observer thread checks the termination event in every
            # _CHECK_FOR_TERMINATION_DELAY seconds. We need to wait at least
            # more than that to make sure that it has a chance to terminate
            # properly.
            io_observer.join(timeout=_CHECK_FOR_TERMINATION_DELAY * 3)
        except TimeoutError:
            raise RuntimeError("Log observers did not terminate in time.")


@lru_cache(maxsize=None)
def sha256_digest_of(*unique_fields: str | bytes) -> str:
    """Return the SHA256 digest that corresponds to the combined version
    of 'unique_fields. The order is preserved."""

    def _normalize(text: str | bytes) -> bytes:
        if isinstance(text, str):
            return text.encode()
        else:
            return text

    join_char = b"\n"
    inner_text = join_char.join(map(_normalize, unique_fields))
    return hashlib.sha256(inner_text).hexdigest()


def active_python() -> str:
    """Return the active Python version that can be used for caching
    and re-creating this environment. Currently only covers major and
    minor versions (like 3.9); patch versions are ignored (like 3.9.4)."""
    return sysconfig.get_python_version()


def optional_import(module_name: str) -> ModuleType:
    """Try to import the given module, and fail if it is not available
    with an informative error message that includes the installations
    instructions."""

    import importlib

    try:
        return importlib.import_module(module_name)
    except ImportError as exc:
        raise ImportError(
            "isolate must be installed with the 'build' extras for "
            f"accessing {module_name!r} import functionality. Please try: "
            f"'$ pip install \"isolate[build]\"' to install it."
        ) from exc


@lru_cache(4)
def get_executable(command: str, home: str | None = None) -> Path:
    for path in [home, None]:
        binary_path = shutil.which(command, path=path)
        if binary_path is not None:
            return Path(binary_path)
    # TODO: we should probably show some instructions on how you
    # can install conda here.
    raise FileNotFoundError(
        f"Could not find the {command} executable. "
        f"If the {command} executable is not available by default, please point "
        f"isolate to the path where the {command} binary is available '{home}'."
    )
