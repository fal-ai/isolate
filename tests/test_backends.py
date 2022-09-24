import subprocess
import textwrap
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import Any

import pytest

from isolate import BaseEnvironment
from isolate.backends.conda import CondaEnvironment, _get_conda_executable
from isolate.backends.virtual_env import VirtualPythonEnvironment
from isolate.common import get_executable_path
from isolate.context import _Context


class NoNewEnvironments(Exception):
    pass


class GenericCreationTests:
    """Generic tests related to environment creation that most
    of the backends can share easily."""

    def get_environment_for(self, cache_path: Any, name: str) -> BaseEnvironment:
        if name not in self.configs:
            raise ValueError(
                f"{type(self).__name__} does not define a configuration for: {name}"
            )

        config = self.configs[name]
        environment = self.backend_cls.from_config(config)

        test_context = _Context(Path(cache_path))
        environment.set_context(test_context)
        return environment

    def get_example_project_version(
        self, environment: BaseEnvironment, connection_key: Any
    ) -> str:
        with environment.open_connection(connection_key) as connection:
            return connection.run(partial(eval, "__import__('pyjokes').__version__"))

    @contextmanager
    def disable_active_env_creation(self, monkeypatch):
        def _raise_error():
            raise NoNewEnvironments

        with monkeypatch.context() as ctx:
            ctx.setattr(
                self.creation_entry_point, lambda *args, **kwargs: _raise_error()
            )
            yield

    def test_create_generic_env(self, tmp_path):
        environment = self.get_environment_for(tmp_path, "new-example-project")
        connection_key = environment.create()
        assert self.get_example_project_version(environment, connection_key) == "0.6.0"

    def test_create_generic_env_empty(self, tmp_path):
        environment = self.get_environment_for(tmp_path, "empty")
        connection_key = environment.create()
        with pytest.raises(ModuleNotFoundError):
            self.get_example_project_version(environment, connection_key)

    def test_create_generic_env_cached(self, tmp_path, monkeypatch):
        environment_1 = self.get_environment_for(tmp_path, "old-example-project")
        environment_2 = self.get_environment_for(tmp_path, "new-example-project")

        # Duplicate environments (though different instances)
        dup_environment_1 = self.get_environment_for(tmp_path, "old-example-project")
        dup_environment_2 = self.get_environment_for(tmp_path, "new-example-project")

        # Create the original environments
        connection_key_1 = environment_1.create()
        connection_key_2 = environment_2.create()

        # This function should prevent the given environment to be "actively"
        # created (e.g. it would break pip install or conda), so that we can
        # verify environments are actually cached.
        with self.disable_active_env_creation(monkeypatch):
            # Can't create it an environment with the same set of dependencies
            # unless explicitly flagged to do so.
            with pytest.raises(FileExistsError):
                dup_environment_1.create()

            # Let's try it again on the first one, but this time with passing `exist_ok`
            dup_connection_key_1 = dup_environment_1.create(exist_ok=True)

        # This time we'll destroy the original environment (env_2) and then
        # try to create the duplicate one, which should fail since active environment
        # creations are not allowed in this context.
        environment_2.destroy(connection_key_2)
        with self.disable_active_env_creation(monkeypatch):
            with pytest.raises(NoNewEnvironments):
                dup_environment_2.create()

        # But once we are out of the disabling context, we can create it again
        dup_connection_key_2 = dup_environment_2.create()

        assert (
            self.get_example_project_version(environment_1, connection_key_1) == "0.5.0"
        )
        assert (
            self.get_example_project_version(dup_environment_1, dup_connection_key_1)
            == "0.5.0"
        )
        assert (
            self.get_example_project_version(dup_environment_2, dup_connection_key_2)
            == "0.6.0"
        )

    def test_failure_during_environment_creation_cache(self, tmp_path, monkeypatch):
        # Let's try to create a faulty environment, and ensure
        # that there are no artifacts left behind in the cache.
        environment = self.get_environment_for(tmp_path, "new-example-project")
        with pytest.raises(Exception):
            with self.disable_active_env_creation(monkeypatch):
                environment.create()

        # It should be able to create it without passing exist_ok
        # since the initial attempt failed.
        connection_key = environment.create()
        assert self.get_example_project_version(environment, connection_key) == "0.6.0"


class TestVenv(GenericCreationTests):

    backend_cls = VirtualPythonEnvironment
    configs = {
        "empty": {
            "requirements": [],
        },
        "old-example-project": {
            "requirements": ["pyjokes==0.5.0"],
        },
        "new-example-project": {
            "requirements": ["pyjokes==0.6.0"],
        },
    }
    creation_entry_point = "virtualenv.cli_run"


# Since conda is an external dependency, we'll skip tests using it
# if it is not installed. v1 vbn
try:
    _get_conda_executable()
except FileNotFoundError:
    IS_CONDA_AVAILABLE = False
else:
    IS_CONDA_AVAILABLE = True


@pytest.mark.skipif(not IS_CONDA_AVAILABLE, reason="Conda is not available")
class TestConda(GenericCreationTests):

    backend_cls = CondaEnvironment
    configs = {
        "empty": {
            # Empty environments on conda does not have Python
            # pre-installed.
            "packages": ["python"],
        },
        "old-example-project": {
            "packages": ["pyjokes=0.5.0"],
        },
        "new-example-project": {
            "packages": ["pyjokes=0.6.0"],
        },
    }
    creation_entry_point = "subprocess.check_call"
