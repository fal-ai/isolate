import subprocess
import textwrap
from contextlib import contextmanager
from pathlib import Path

import pytest

from isolate.backends.virtual_env import VirtualPythonEnvironment
from isolate.common import get_executable_path


@pytest.fixture
def temp_cache_dir(monkeypatch, tmp_path):
    new_venv_cache = tmp_path / "venvs"
    monkeypatch.setattr("isolate.backends.virtual_env._BASE_VENV_DIR", new_venv_cache)


class NoNewEnvironments(Exception):
    pass


@contextmanager
def _make_new_virtual_env_fail(monkeypatch):
    def _raise_error():
        raise NoNewEnvironments

    with monkeypatch.context() as ctx:
        ctx.setattr("virtualenv.cli_run", lambda *args, **kwargs: _raise_error())
        yield


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
def test_virtual_env_caching(temp_cache_dir, monkeypatch):
    env_1 = VirtualPythonEnvironment(["pyjokes==0.5.0"])
    env_2 = VirtualPythonEnvironment(["pyjokes==0.6.0"])
    dup_env_1 = VirtualPythonEnvironment(["pyjokes==0.5.0"])
    dup_env_2 = VirtualPythonEnvironment(["pyjokes==0.6.0"])

    env_1_path = env_1.create()
    env_2_path = env_2.create()

    with _make_new_virtual_env_fail(monkeypatch):
        # Can't create it an environment with the same set of dependencies
        # unless explicitly flagged to do so.
        with pytest.raises(FileExistsError):
            dup_env_1.create()

        # Let's try it again on the first one, but this time with passing `exist_ok`
        dup_env_1_path = dup_env_1.create(exist_ok=True)

    # This time we'll destroy the original environment (env_2) and then
    # try to duplicate one, which should fail since new environment creations
    # are not allowed in this context.

    env_2.destroy(env_2_path)
    with _make_new_virtual_env_fail(monkeypatch):
        with pytest.raises(NoNewEnvironments):
            dup_env_2.create()
    env_2_path = env_2.create()

    assert _get_pyjokes_version(env_1_path) == "0.5.0"
    assert _get_pyjokes_version(env_2_path) == "0.6.0"
    assert _get_pyjokes_version(dup_env_1_path) == "0.5.0"

    assert env_1_path == dup_env_1_path
    assert env_2_path != dup_env_1_path


@pytest.mark.requires_internet
def test_failure_during_environment_creation_cache(temp_cache_dir, monkeypatch):
    # Let's try to create a faulty environment, and ensure
    # that there are no artifacts left behind in the cache.

    env_1 = VirtualPythonEnvironment(["pyjokes==0.6.0"])
    with pytest.raises(Exception):
        with _make_new_virtual_env_fail(monkeypatch):
            env_1.create()

    # I should be able to create it without passing exist_ok
    # since the environment should not exist at all.
    env_1_path = env_1.create()
    assert _get_pyjokes_version(env_1_path) == "0.6.0"
