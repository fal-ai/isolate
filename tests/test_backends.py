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

    def get_example_version(
        self, environment: BaseEnvironment, connection_key: Any
    ) -> str:
        with environment.open_connection(connection_key) as connection:
            return connection.run(partial(eval, "__import__('pyjokes').__version__"))

    @contextmanager
    def fail_active_creation(self, monkeypatch):
        # This function patches the specified entry_point function
        # in order to make non-cached (active) environment creations
        # fail on tests.

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
        assert self.get_example_version(environment, connection_key) == "0.6.0"

    def test_create_generic_env_empty(self, tmp_path):
        environment = self.get_environment_for(tmp_path, "empty")
        connection_key = environment.create()
        with pytest.raises(ModuleNotFoundError):
            self.get_example_version(environment, connection_key)

    def test_create_generic_env_cached(self, tmp_path, monkeypatch):
        environment_1 = self.get_environment_for(tmp_path, "old-example-project")
        environment_2 = self.get_environment_for(tmp_path, "new-example-project")

        # Duplicate environments (though different instances)
        dup_environment_1 = self.get_environment_for(tmp_path, "old-example-project")
        dup_environment_2 = self.get_environment_for(tmp_path, "new-example-project")

        # Create the original environments
        connection_key_1 = environment_1.create()
        connection_key_2 = environment_2.create()

        # Since the environments are identified by the set of unique properties
        # they have (e.g. the installed dependencies), if two environments have
        # the same set of properties, they should be considered the same by the
        # implementation. Even though we didn't call create() on the following
        # environment, it still exists due to the caching mechanism.
        assert dup_environment_1.exists()

        # We can also see that if we destroy it, both the original one and the duplicate
        # one will be gone.
        environment_1.destroy(connection_key_1)

        assert not dup_environment_1.exists()
        assert not environment_1.exists()

        connection_key_1 = environment_1.create()
        dup_connection_key_1 = dup_environment_1.create()
        dup_connection_key_2 = dup_environment_2.create()

        for environment, connection_key, version in [
            (environment_1, connection_key_1, "0.5.0"),
            (environment_2, connection_key_2, "0.6.0"),
            (dup_environment_1, dup_connection_key_1, "0.5.0"),
            (dup_environment_2, dup_connection_key_2, "0.6.0"),
        ]:
            assert self.get_example_version(environment, connection_key) == version

    def test_failure_during_environment_creation_cache(self, tmp_path, monkeypatch):
        environment = self.get_environment_for(tmp_path, "new-example-project")
        with pytest.raises(NoNewEnvironments):
            with self.fail_active_creation(monkeypatch):
                environment.create()

        assert not environment.exists()

        connection_key = environment.create()
        assert environment.exists()
        assert self.get_example_version(environment, connection_key) == "0.6.0"


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
