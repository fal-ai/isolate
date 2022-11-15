import subprocess
import sys
from contextlib import contextmanager
from functools import partial
from os import environ
from pathlib import Path
from typing import Any, Dict, List, Type

import pytest

import isolate
from isolate.backends import BaseEnvironment, EnvironmentCreationError
from isolate.backends.common import sha256_digest_of
from isolate.backends.conda import CondaEnvironment, _get_conda_executable
from isolate.backends.local import LocalPythonEnvironment
from isolate.backends.remote import IsolateServer
from isolate.backends.settings import IsolateSettings
from isolate.backends.virtualenv import VirtualPythonEnvironment


class GenericEnvironmentTests:
    """Generic tests related to environment management that most
    of the backends can share easily."""

    backend_cls: Type[BaseEnvironment]
    configs: Dict[str, Dict[str, Any]]

    def get_project_environment(self, tmp_path: Any, name: str) -> BaseEnvironment:
        if name not in self.configs:
            raise ValueError(
                f"{type(self).__name__} does not define a configuration for: {name}"
            )

        config = self.configs[name]
        return self.get_environment(tmp_path, config)

    def get_environment(self, tmp_path: Any, config: Dict[str, Any]) -> BaseEnvironment:
        environment = self.backend_cls.from_config(config)

        test_settings = IsolateSettings(Path(tmp_path))
        environment.apply_settings(test_settings)
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

    @pytest.mark.parametrize("executable", ["python"])
    def test_path_resolution(self, tmp_path, executable):
        import shutil

        environment = self.get_project_environment(tmp_path, "example-binary")
        environment_path = environment.create()
        with environment.open_connection(environment_path) as connection:
            executable_path = Path(connection.run(partial(shutil.which, executable)))
            assert executable_path.exists()
            assert executable_path.relative_to(environment_path)


class TestVirtualenv(GenericEnvironmentTests):

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
        "example-binary": {
            "requirements": [],
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
class TestConda(GenericEnvironmentTests):

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
        "r": {
            "packages": ["r-base=4.2.2"],
        },
        "example-binary": {
            "packages": ["python"],
        },
    }
    creation_entry_point = ("subprocess.check_call", subprocess.SubprocessError)

    def test_invalid_project_building(self):
        pytest.xfail(
            "For a weird reason, installing an invalid package on conda does "
            "not make it exit with an error code."
        )

    def test_conda_binary_execution(self, tmp_path):
        environment = self.get_project_environment(tmp_path, "r")
        environment_path = environment.create()
        r_binary = environment_path / "bin" / "R"
        assert r_binary.exists()

        output = subprocess.check_output([r_binary, "--version"], text=True)
        assert "R version 4.2.2" in output


def test_local_python_environment():
    """Since 'local' environment does not support installation of extra dependencies
    unlike virtualenv/conda, we can't use the generic test suite for it."""

    local_env = LocalPythonEnvironment()
    assert local_env.exists()

    connection_key = local_env.create()
    with local_env.open_connection(connection_key) as connection:
        assert (
            connection.run(partial(eval, "__import__('sys').exec_prefix"))
            == sys.exec_prefix
        )

    with pytest.raises(NotImplementedError):
        local_env.destroy(connection_key)


def test_path_on_local():
    import shutil

    local_env = LocalPythonEnvironment()
    with local_env.connect() as connection:
        assert connection.run(partial(shutil.which, "python")) == shutil.which("python")


def test_isolate_server_environment(isolate_server):
    environment = IsolateServer(
        host=isolate_server,
        target_environments=[
            {
                "kind": "virtualenv",
                "configuration": {"requirements": ["pyjokes==0.5.0"]},
            }
        ],
    )

    connection_key = environment.create()
    with environment.open_connection(connection_key) as connection:
        assert (
            connection.run(partial(eval, "__import__('pyjokes').__version__"))
            == "0.5.0"
        )


def test_isolate_server_on_conda(isolate_server):
    environment_2 = IsolateServer(
        host=isolate_server,
        target_environments=[
            {
                "kind": "conda",
                "configuration": {
                    "packages": [
                        # Match the same python version (since inherit local=
                        # is True).
                        f"python={'.'.join(map(str, sys.version_info[:2]))}",
                        "pyjokes=0.6.0",
                    ]
                },
            }
        ],
    )
    with environment_2.connect() as connection:
        assert (
            connection.run(partial(eval, "__import__('pyjokes').__version__"))
            == "0.6.0"
        )


def test_isolate_server_logs(isolate_server):
    collected_logs = []
    environment = IsolateServer(
        host=isolate_server,
        target_environments=[
            {
                "kind": "virtualenv",
                "configuration": {"requirements": ["pyjokes==0.5.0"]},
            }
        ],
    )
    environment.apply_settings(IsolateSettings(log_hook=collected_logs.append))

    with environment.connect() as connection:
        connection.run(partial(eval, "print('hello!!!')"))

    assert "hello!!!" in [log.message for log in collected_logs]


def test_isolate_server_demo(isolate_server):
    from functools import partial

    environments = {
        "dev": {
            "kind": "virtualenv",
            "configuration": {
                "requirements": [
                    "pyjokes==0.6.0",
                ]
            },
        },
        "prod": {
            "kind": "virtualenv",
            "configuration": {
                "requirements": [
                    "pyjokes==0.5.0",
                ]
            },
        },
    }

    for definition in environments.values():
        local_environment = isolate.prepare_environment(
            definition["kind"],
            **definition["configuration"],
        )
        remote_environment = isolate.prepare_environment(
            "isolate-server", host=isolate_server, target_environments=[definition]
        )
        with local_environment.connect() as local_connection, remote_environment.connect() as remote_connection:
            for target_func in [
                partial(eval, "__import__('pyjokes').__version__"),
                partial(eval, "2 + 2"),
            ]:
                assert local_connection.run(target_func) == remote_connection.run(
                    target_func
                )


def test_isolate_server_multiple_envs(isolate_server):
    from functools import partial

    environment_configs = [
        {
            "kind": "virtualenv",
            "configuration": {
                "requirements": [
                    "python-dateutil==2.8.2",
                ]
            },
        },
        {
            "kind": "virtualenv",
            "configuration": {
                "requirements": [
                    "pyjokes==0.5.0",
                ]
            },
        },
    ]

    environment = IsolateServer(
        host=isolate_server, target_environments=environment_configs
    )

    connection_key = environment.create()
    with environment.open_connection(connection_key) as connection:
        assert (
            connection.run(
                partial(
                    eval,
                    "__import__('pyjokes').__version__ + ' ' + __import__('dateutil').__version__",
                )
            )
            == "0.5.0 2.8.2"
        )


@pytest.mark.parametrize(
    "kind, config",
    [
        (
            "virtualenv",
            {
                "packages": [
                    "pyjokes==1.0.0",
                ]
            },
        ),
        (
            "conda",
            {
                "requirements": [
                    "pyjokes=1.0.0",
                ]
            },
        ),
        (
            "isolate-server",
            {
                "host": "localhost",
                "port": 1234,
                "target_environment_kind": "virtualenv",
                "target_environment_config": {
                    "requirements": [
                        "pyjokes==1.0.0",
                    ]
                },
            },
        ),
    ],
)
def test_wrong_options(kind, config):
    with pytest.raises(TypeError):
        isolate.prepare_environment(kind, **config)
