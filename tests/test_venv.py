import subprocess
import textwrap
from pathlib import Path

import pytest

from isolate.backends.virtual_env import VirtualPythonEnvironment
from isolate.common import get_executable_path


@pytest.fixture
def temp_cache_dir(monkeypatch, tmp_path):
    new_venv_cache = tmp_path / "venvs"
    monkeypatch.setattr("isolate.backends.virtual_env._BASE_VENV_DIR", new_venv_cache)


def _get_pyjokes_version(environment_path: Path) -> str:
    python_executable = get_executable_path(environment_path, "python")
    execution_result = subprocess.check_output(
        [
            python_executable,
            "-c",
            textwrap.dedent(
                """
            try:
                import pyjokes
            except ImportError:
                print("is not installed")
            else:
                print(pyjokes.__version__)
            """
            ),
        ],
        text=True,
    )
    return execution_result.strip()


def test_virtual_env_create(temp_cache_dir):
    env = VirtualPythonEnvironment([])
    path = env.create()
    assert _get_pyjokes_version(path) == "is not installed"


@pytest.mark.requires_internet
def test_virtual_env_create_with_requirements(temp_cache_dir):
    env = VirtualPythonEnvironment(["pyjokes==0.5.0"])
    path = env.create()
    assert _get_pyjokes_version(path) == "0.5.0"


@pytest.mark.requires_internet
def test_virtual_env_caching(temp_cache_dir):
    env_1 = VirtualPythonEnvironment(["pyjokes==0.5.0"])
    env_2 = VirtualPythonEnvironment(["pyjokes==0.6.0"])
    env_3 = VirtualPythonEnvironment(["pyjokes==0.6.0"])

    env_1_path = env_1.create()
    env_2_path = env_2.create()

    # Can't create it an environment with the same set of dependencies
    # unless explicitly flagged to do so.
    with pytest.raises(FileExistsError):
        env_3_path = env_3.create()

    env_3_path = env_3.create(exist_ok=True)

    assert _get_pyjokes_version(env_1_path) == "0.5.0"
    assert _get_pyjokes_version(env_2_path) == "0.6.0"
    assert _get_pyjokes_version(env_3_path) == "0.6.0"

    assert env_1_path != env_2_path
    assert env_1_path != env_3_path
    assert env_2_path == env_3_path


@pytest.mark.requires_internet
def test_failure_during_environment_creation_cache(temp_cache_dir, monkeypatch):
    # Let's try to create a faulty environment, and ensure
    # that there are no artifacts left behind in the cache.

    env_1 = VirtualPythonEnvironment(["pyjokes==0.6.0"])
    with pytest.raises(Exception):
        with monkeypatch.context() as ctx:
            ctx.setattr("virtualenv.cli_run", lambda *args, **kwargs: 1 / 0)
            env_1.create()

    # I should be able to create it without passing exist_ok
    # since the environment should not exist at all.
    env_1_path = env_1.create()
    assert _get_pyjokes_version(env_1_path) == "0.6.0"
