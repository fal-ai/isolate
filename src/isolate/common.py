import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from platformdirs import user_cache_dir

BASE_CACHE_DIR = Path(user_cache_dir("isolate", "isolate"))


@contextmanager
def rmdir_on_fail(path: Path) -> Iterator[None]:
    try:
        yield
    except Exception:
        if path.exists():
            shutil.rmtree(path)
        raise


def get_executable_path(search_path: Path, executable_name: str) -> Path:
    bin_dir = (search_path / "bin").as_posix()
    executable_path = shutil.which(executable_name, path=bin_dir)
    if executable_path is None:
        raise FileNotFoundError(
            f"Could not find '{executable_name}' in '{search_path}'. "
            f"Is the virtual environment corrupted?"
        )

    return Path(executable_path)
