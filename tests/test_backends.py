import re
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
from isolate.backends.pyenv import PyenvEnvironment, _get_pyenv_executable
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

    def get_python_version(
        self, environment: BaseEnvironment, connection_key: Any
    ) -> str:
        with environment.open_connection(connection_key) as connection:
            return connection.run(partial(eval, "sys.version"))

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

    def _run_cmd(self, connection, executable, *cmd_args):
        import subprocess

        cmd = [executable, *cmd_args]
        return connection.run(
            partial(
                subprocess.check_output,
                cmd,
                text=True,
                stderr=subprocess.STDOUT,
            )
        )

    def test_executable_running(self, tmp_path):
        environment = self.get_project_environment(tmp_path, "example-binary")
        with environment.connect() as connection:
            py_version = self._run_cmd(connection, "python", "--version")
            assert py_version.startswith("Python 3")

    def test_self_installed_executable_running(self, tmp_path):
        environment = self.get_project_environment(tmp_path, "dbt-core")
        with environment.connect() as connection:
            dbt_version = self._run_cmd(connection, "dbt", "--version")
            assert "installed: 1.3.1" in dbt_version

    def test_custom_python_version(self, tmp_path):
        for python_type, python_version in [
            ("old-python", "3.7"),
            ("new-python", "3.10"),
        ]:
            environment = self.get_project_environment(tmp_path, python_type)
            python_version = self.get_python_version(environment, environment.create())
            assert python_version.startswith(python_version)


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
        "dbt-core": {
            "requirements": ["dbt-core==1.3.1"],
        },
        "old-python": {
            "python_version": "3.7",
        },
        "new-python": {
            "python_version": "3.10",
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

    def test_extra_index_urls(self, tmp_path):
        # Only available in test.pypi.org
        alpha_version = "2022.9.26a0"
        environment = self.get_environment(
            tmp_path,
            {"requirements": [f"black=={alpha_version}"]},
        )

        # This should fail since the default index server doesn't have
        # the alpha version of  black
        with pytest.raises(EnvironmentCreationError):
            environment.create()

        # This should work since we are using the test.pypi.org server
        environment = self.get_environment(
            tmp_path,
            {
                "requirements": [f"black=={alpha_version}"],
                "extra_index_urls": ["https://test.pypi.org/simple/"],
            },
        )
        with environment.connect() as connection:
            assert alpha_version in self._run_cmd(connection, "black", "--version")

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

    def test_custom_python_version(self, tmp_path, monkeypatch):
        # Disable pyenv to prevent auto-installation
        monkeypatch.setattr(
            "isolate.backends.pyenv._get_pyenv_executable", lambda: 1 / 0
        )

        for python_type, expected_python_version in [
            ("old-python", "3.7"),
            ("new-python", "3.10"),
        ]:
            environment = self.get_project_environment(tmp_path, python_type)
            try:
                connection = environment.create()
            except EnvironmentCreationError:
                pytest.skip(
                    "This python version not available on the system (through virtualenv)"
                )

            python_version = self.get_python_version(environment, connection)
            assert python_version.startswith(expected_python_version)

    def test_invalid_python_version_raises(self, tmp_path, monkeypatch):
        # Disable pyenv to prevent auto-installation
        monkeypatch.setattr(
            "isolate.backends.pyenv._get_pyenv_executable", lambda: 1 / 0
        )

        # Hopefully there will never be a Python 9.9.9
        environment = self.get_environment(tmp_path, {"python_version": "9.9.9"})
        with pytest.raises(
            EnvironmentCreationError,
            match="Python 9.9.9 is not available",
        ):
            environment.create()


# Since conda is an external dependency, we'll skip tests using it
# if it is not installed.
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
            "packages": [],
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
            "packages": [],
        },
        "dbt-core": {
            "packages": ["dbt-core=1.3.1"],
        },
        "old-python": {
            "python_version": "3.7",
        },
        "new-python": {
            "python_version": "3.10",
        },
        "yml-with-isolate": {
            "env_yml_str": 'name: test\n' + \
            'channels:\n  - defaults\n' + \
            'dependencies:\n  - pip:\n    - isolate==0.7.1\n    - pyjokes==0.5.0\n'
        },
        "yml-without-isolate": {
            "env_yml_str": 'name: test\n' + \
            'channels:\n  - defaults\n' + \
            'dependencies:\n  - pip:\n    - pyjokes==0.5.0\n'
        }
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

    @pytest.mark.parametrize(
        "user_packages",
        [
            ["python"],
            ["python=3.7"],
            ["python>=3.7"],
            ["python=3.7.*"],
            ["python=3.7.10"],
            ["python != 3.7"],
            ["python> 3.7"],
            ["python <3.7"],
            ["pyjokes", "python>=3.7", "emoji"],
            ["python<=3.7", "emoji"],
            ["pyjokes", "python==3.7"],
        ],
    )
    @pytest.mark.parametrize("python_version", [None, "3.9"])
    def test_fail_when_user_overwrites_python(
        self, tmp_path, user_packages, python_version
    ):
        environment = self.get_environment(
            tmp_path,
            {
                "packages": user_packages,
                "python_version": python_version,
            },
        )
        with pytest.raises(
            EnvironmentCreationError,
            match="Python version can not be specified by packages",
        ):
            environment.create()

    def test_fail_when_yml_without_isolate(self, tmp_path):
        with pytest.raises(
            EnvironmentCreationError,
            match="isolate not found in environment YAML file. Please add isolate as a pip dependency."
        ):
            environment = self.get_project_environment(tmp_path, "yml-without-isolate")
            assert environment is None # Should not reach this line

    def test_environment_with_yml(self, tmp_path):
        environment = self.get_project_environment(tmp_path, "yml-with-isolate")
        connection_key = environment.create()
        assert self.get_example_version(environment, connection_key) == "0.5.0"

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


# Since pyenv is an external dependency, we'll skip tests using it
# if it is not installed.
try:
    _get_pyenv_executable()
except FileNotFoundError:
    IS_PYENV_AVAILABLE = False
else:
    IS_PYENV_AVAILABLE = True


@pytest.mark.skipif(not IS_PYENV_AVAILABLE, reason="Pyenv is not available")
@pytest.mark.parametrize("python_version", ["3.7", "3.10", "3.9.15"])
def test_pyenv_environment(python_version, tmp_path):
    different_python = PyenvEnvironment(python_version)
    test_settings = IsolateSettings(Path(tmp_path))
    different_python.apply_settings(test_settings)

    assert not different_python.exists()

    connection_key = different_python.create()
    assert different_python.exists()

    with different_python.open_connection(connection_key) as connection:
        assert connection.run(partial(eval, "__import__('sys').version")).startswith(
            python_version
        )

    # Do a cached run.
    with different_python.open_connection(connection_key) as connection:
        assert connection.run(partial(eval, "__import__('sys').version")).startswith(
            python_version
        )

    different_python.destroy(connection_key)
    assert not different_python.exists()


@pytest.mark.skipif(not IS_PYENV_AVAILABLE, reason="Pyenv is not available")
def test_virtual_env_custom_python_version_with_pyenv(tmp_path, monkeypatch):
    pyjokes_env = VirtualPythonEnvironment(
        requirements=["pyjokes==0.6.0"],
        python_version="3.9",
    )

    test_settings = IsolateSettings(Path(tmp_path))
    pyjokes_env.apply_settings(test_settings)

    # Force it to choose pyenv as the python version manager.
    pyjokes_env._decide_python = pyjokes_env._install_python_through_pyenv

    connection_key = pyjokes_env.create()
    with pyjokes_env.open_connection(connection_key) as connection:
        assert connection.run(partial(eval, "__import__('sys').version")).startswith(
            "3.9"
        )
        assert (
            connection.run(partial(eval, "__import__('pyjokes').__version__"))
            == "0.6.0"
        )
