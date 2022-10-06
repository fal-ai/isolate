import subprocess
import textwrap
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import Any, Dict, List

import pytest

from isolate.backends import BaseEnvironment, EnvironmentCreationError
from isolate.backends.common import get_executable_path, sha256_digest_of
from isolate.backends.conda import CondaEnvironment, _get_conda_executable
from isolate.backends.context import _Context
from isolate.backends.virtual_env import VirtualPythonEnvironment


class GenericCreationTests:
    """Generic tests related to environment creation that most
    of the backends can share easily."""

    def get_project_environment(self, tmp_path: Any, name: str) -> BaseEnvironment:
        if name not in self.configs:
            raise ValueError(
                f"{type(self).__name__} does not define a configuration for: {name}"
            )

        config = self.configs[name]
        return self.get_environment(tmp_path, config)

    def get_environment(self, tmp_path: Any, config: Dict[str, Any]) -> BaseEnvironment:
        environment = self.backend_cls.from_config(config)

        test_context = _Context(Path(tmp_path))
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

        entry_point, exc_class = self.creation_entry_point

        def _raise_error():
            raise exc_class

        with monkeypatch.context() as ctx:
            ctx.setattr(entry_point, lambda *args, **kwargs: _raise_error())
            yield

    def test_create_generic_env(self, tmp_path):
        environment = self.get_project_environment(tmp_path, "new-example-project")
        connection_key = environment.create()
        assert self.get_example_version(environment, connection_key) == "0.6.0"

    def test_create_generic_env_empty(self, tmp_path):
        environment = self.get_project_environment(tmp_path, "empty")
        connection_key = environment.create()
        with pytest.raises(ModuleNotFoundError):
            self.get_example_version(environment, connection_key)

    def test_create_generic_env_cached(self, tmp_path, monkeypatch):
        environment_1 = self.get_project_environment(tmp_path, "old-example-project")
        environment_2 = self.get_project_environment(tmp_path, "new-example-project")

        # Duplicate environments (though different instances)
        dup_environment_1 = self.get_project_environment(
            tmp_path, "old-example-project"
        )
        dup_environment_2 = self.get_project_environment(
            tmp_path, "new-example-project"
        )

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
        environment = self.get_project_environment(tmp_path, "new-example-project")
        with pytest.raises(EnvironmentCreationError):
            with self.fail_active_creation(monkeypatch):
                environment.create()

        assert not environment.exists()

        connection_key = environment.create()
        assert environment.exists()
        assert self.get_example_version(environment, connection_key) == "0.6.0"

    def test_invalid_project_building(self, tmp_path, monkeypatch):
        environment = self.get_project_environment(tmp_path, "invalid-project")
        with pytest.raises(EnvironmentCreationError):
            environment.create()

        assert not environment.exists()


class TestVirtualenv(GenericCreationTests):

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
        "invalid-project": {
            "requirements": ["pyjokes==999.999.999"],
        },
    }
    creation_entry_point = ("virtualenv.cli_run", PermissionError)

    def make_constraints_file(self, tmp_path: Any, constraints: List[str]) -> Any:
        constraints_file = (
            tmp_path / f"constraints_{sha256_digest_of(*constraints)}.txt"
        )
        constraints_file.write_text("\n".join(constraints))
        return constraints_file

    def test_constraints(self, tmp_path):
        contraints_file = self.make_constraints_file(
            tmp_path, ["pyjokes>=0.4.0,<0.6.0"]
        )
        environment = self.get_environment(
            tmp_path,
            {"requirements": ["pyjokes>=0.4.1"], "constraints_file": contraints_file},
        )
        connection_key = environment.create()

        # The only versions that satisfy the given constraints are 0.4.1 and 0.5.0
        # and pip is going to pick the latest one.
        assert self.get_example_version(environment, connection_key) == "0.5.0"

    def test_unresolvable_constraints(self, tmp_path):
        contraints_file = self.make_constraints_file(tmp_path, ["pyjokes>=0.6.0"])
        environment = self.get_environment(
            tmp_path,
            {"requirements": ["pyjokes<0.6.0"], "constraints_file": contraints_file},
        )

        # When we can't find a version that satisfies all the constraints, we
        # are going to abort early to let you know.
        with pytest.raises(EnvironmentCreationError):
            environment.create()

        # Ensure that the same environment works when we remove the constraints.
        environment = self.get_environment(
            tmp_path,
            {"requirements": ["pyjokes<0.6.0"]},
        )
        connection_key = environment.create()
        assert self.get_example_version(environment, connection_key) == "0.5.0"

    def test_caching_with_constraints(self, tmp_path):
        contraints_file_1 = self.make_constraints_file(tmp_path, ["pyjokes>=0.6.0"])
        contraints_file_2 = self.make_constraints_file(tmp_path, ["pyjokes<=0.6.0"])

        environment_1 = self.get_environment(
            tmp_path,
            {"requirements": ["pyjokes<0.6.0"]},
        )
        environment_2 = self.get_environment(
            tmp_path,
            {"requirements": ["pyjokes<0.6.0"], "constraints_file": contraints_file_1},
        )
        environment_3 = self.get_environment(
            tmp_path,
            {"requirements": ["pyjokes<0.6.0"], "constraints_file": contraints_file_2},
        )
        assert environment_1.key != environment_2.key != environment_3.key


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
        "invalid-project": {
            "requirements": ["pyjokes=999.999.999"],
        },
    }
    creation_entry_point = ("subprocess.check_call", subprocess.SubprocessError)

    def test_invalid_project_building(self):
        pytest.xfail(
            "For a weird reason, installing an invalid package on conda does "
            "not make it exit with an error code."
        )
