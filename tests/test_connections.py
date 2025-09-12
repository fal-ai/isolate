import operator
import subprocess
import sys
import threading
import time
import traceback
from contextlib import ExitStack
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import Any, List
from unittest.mock import Mock

import pytest
from isolate.backends import BaseEnvironment, EnvironmentConnection
from isolate.backends.local import LocalPythonEnvironment
from isolate.backends.settings import IsolateSettings
from isolate.backends.virtualenv import VirtualPythonEnvironment
from isolate.connections import LocalPythonGRPC, PythonIPC
from isolate.connections.common import is_agent

REPO_DIR = Path(__file__).parent.parent
assert (
    REPO_DIR.exists() and REPO_DIR.name == "isolate"
), "This test should have access to isolate as an installable package."


# Enable dill to only serialize globals that are accessed by the function
import dill  # noqa: E402

dill.settings["recurse"] = True


class GenericPythonConnectionTests:
    """Generic tests for local Python connection implementations."""

    def open_connection(
        self,
        environment: BaseEnvironment,
        environment_path: Path,
        **kwargs: Any,
    ) -> EnvironmentConnection:
        """Open a new connection (to be implemented by various connection
        types for testing all of them)."""
        raise NotImplementedError

    def make_venv(
        self, tmp_path: Any, requirements: List[str]
    ) -> VirtualPythonEnvironment:
        """Create a new virtual env with the specified requirements."""
        env = VirtualPythonEnvironment(requirements)
        env.apply_settings(IsolateSettings(Path(tmp_path)))
        return env

    def check_version(self, connection: EnvironmentConnection, module: str) -> str:
        """Return the version for the given module."""
        src = f"__import__('{module}').__version__"
        return connection.run(partial(eval, src))

    def test_basic_connection(self):
        local_env = LocalPythonEnvironment()

        with self.open_connection(local_env, local_env.create()) as conn:
            result = conn.run(partial(operator.add, 1, 2))
            assert result == 3

    @pytest.mark.parametrize("serialization_method", ["dill"])
    def test_customized_serialization(self, serialization_method: str) -> None:
        local_env = LocalPythonEnvironment()
        assert local_env.settings.serialization_method == "pickle"

        # By default the serialization uses pickle, which can't serialize
        # a lambda function.
        with self.open_connection(local_env, local_env.create()) as conn:
            with pytest.raises(Exception):
                result: int = conn.run(lambda: 1 + 2)

        # But we can switch serialization backends, and use cloudpickle or dill
        # since both of them can serialize a lambda function.
        cloudpickle_ettings = replace(
            local_env.settings,
            serialization_method=serialization_method,
        )
        local_env.apply_settings(cloudpickle_ettings)

        with self.open_connection(local_env, local_env.create()) as conn:
            result = conn.run(lambda: 1 + 2)
            assert result == 3

    def test_extra_inheritance_paths(self, tmp_path: Any) -> None:
        first_env = self.make_venv(tmp_path, ["pyjokes==0.5.0"])
        second_env = self.make_venv(tmp_path, ["emoji==0.5.4"])

        with self.open_connection(
            first_env,
            first_env.create(),
            extra_inheritance_paths=[second_env.create()],
        ) as conn:
            assert self.check_version(conn, "pyjokes") == "0.5.0"
            assert self.check_version(conn, "emoji") == "0.5.4"

        third_env = self.make_venv(tmp_path, ["pyjokes==0.6.0", "emoji==2.0.0"])
        with self.open_connection(
            second_env,
            second_env.create(),
            extra_inheritance_paths=[third_env.create()],
        ) as conn:
            assert self.check_version(conn, "pyjokes") == "0.6.0"
            # Even if the third environment has a newer version of emoji, it won't be
            # used because since the second environment already has emoji installed and
            # it takes the precedence.
            assert self.check_version(conn, "emoji") == "0.5.4"

        # Order matters, so if the first_env (with 0.5.0) is specified first then it
        # is going to take precedence.
        with self.open_connection(
            first_env,
            first_env.create(),
            extra_inheritance_paths=[third_env.create()],
        ) as conn:
            assert self.check_version(conn, "pyjokes") == "0.5.0"

        # Or if it is specified last, then it will be overridden.
        with self.open_connection(
            third_env,
            third_env.create(),
            extra_inheritance_paths=[first_env.create()],
        ) as conn:
            assert self.check_version(conn, "pyjokes") == "0.6.0"

        fourth_env = self.make_venv(tmp_path, ["pyjokes==0.4.1", "emoji==2.1.0"])

        with self.open_connection(
            first_env,
            first_env.create(),
            extra_inheritance_paths=[third_env.create(), fourth_env.create()],
        ) as conn:
            # This comes from the first_env
            assert self.check_version(conn, "pyjokes") == "0.5.0"
            # This comes from the third_env
            assert self.check_version(conn, "emoji") == "2.0.0"

    def test_is_agent(self):
        local_env = LocalPythonEnvironment()

        assert not is_agent()
        with self.open_connection(local_env, local_env.create()) as conn:
            assert not is_agent()
            assert conn.run(is_agent)
            assert not is_agent()
        assert not is_agent()

    def test_tracebacks(self):
        local_env = LocalPythonEnvironment()
        local_env.apply_settings(
            local_env.settings.replace(serialization_method="dill")
        )

        def long_function_chain():
            def foo():
                a = 1
                b = 0
                c = a / b
                return c

            def bar():
                a = "" + ""  # noqa: F841
                return 0 + foo() + 1

            def baz():
                return bar() + 1

            return baz()

        with self.open_connection(local_env, local_env.create()) as conn:
            with pytest.raises(ZeroDivisionError) as exc:
                conn.run(long_function_chain)

            exception = "".join(
                traceback.format_exception(
                    type(exc.value), exc.value, exc.value.__traceback__
                )
            )
            assert "c = a / b" in exception
            assert "return 0 + foo() + 1" in exception
            assert "return bar() + 1" in exception
            assert "return baz()" in exception
            assert "conn.run(long_function_chain)" in exception


class TestPythonIPC(GenericPythonConnectionTests):
    def open_connection(
        self,
        environment: BaseEnvironment,
        environment_path: Path,
        **kwargs: Any,
    ) -> EnvironmentConnection:
        return PythonIPC(environment, environment_path, **kwargs)


class TestPythonGRPC(GenericPythonConnectionTests):
    def open_connection(
        self,
        environment: BaseEnvironment,
        environment_path: Path,
        **kwargs: Any,
    ) -> EnvironmentConnection:
        return LocalPythonGRPC(environment, environment_path, **kwargs)

    def make_venv(
        self, tmp_path: Any, requirements: List[str]
    ) -> VirtualPythonEnvironment:
        # Since gRPC agent requires isolate to be installed, we
        # have to add it to the requirements.
        env = VirtualPythonEnvironment(requirements + [f"{REPO_DIR}[grpc]"])
        env.apply_settings(IsolateSettings(Path(tmp_path)))
        return env

    def test_process_termination(self):
        local_env = LocalPythonEnvironment()
        connection = LocalPythonGRPC(local_env, local_env.create())

        # Mock terminate_proc on the connection instance before starting agent
        connection.terminate_proc = Mock(wraps=connection.terminate_proc)

        # Use ExitStack to manage the start_agent context manager
        bound_context = ExitStack()
        bound_context.enter_context(connection.start_agent(0.1))

        # Confirm with active context terminate_proc is not called yet
        connection.terminate_proc.assert_not_called()

        # Explicitly close the context to trigger cleanup
        bound_context.close()

        # After closing context, terminate_proc should have been called
        connection.terminate_proc.assert_called_once()

    def test_terminate_proc(self):
        """Test that LocalPythonGRPC.terminate_proc() successfully kills
        a process with SIGTERM."""

        local_env = LocalPythonEnvironment()
        connection = LocalPythonGRPC(local_env, local_env.create())
        # Create a process that responds to SIGTERM
        code = """import signal, time, sys
while True:
    time.sleep(0.1)"""

        proc = subprocess.Popen([sys.executable, "-c", code])
        # Verify process is running initially
        assert proc.poll() is None, "Process should be running initially"

        # terminate_proc() should send SIGTERM and wait for process to die
        connection.terminate_proc(proc, shutdown_grace_period=3.0)

        # Process should be terminated after terminate_proc() returns
        assert proc.poll() is not None, "Process should be terminated by SIGTERM"

    def test_force_terminate(self):
        """Test that LocalPythonGRPC.force_terminate() kills a process immediately."""
        local_env = LocalPythonEnvironment()
        connection = LocalPythonGRPC(local_env, local_env.create())

        # Start a process that ignores SIGTERM
        code = """import signal, time, os
# Set up signal handler to ignore SIGTERM
signal.signal(signal.SIGTERM, signal.SIG_IGN)
# Signal that setup is complete
print("R", flush=True)
while True:
    time.sleep(0.1)"""

        proc = subprocess.Popen(
            [sys.executable, "-c", code],
            stdout=subprocess.PIPE,
        )
        assert proc.poll() is None, "Process should be running initially"

        # Wait for "READY" signal from process because signal handling requires it
        assert proc.stdout is not None
        ready_line = proc.stdout.read(1)
        assert ready_line == b"R"

        # run terminate_proc in background thread since it will block
        # waiting for the process to terminate (which it won't since it ignores SIGTERM)
        threading.Thread(target=connection.terminate_proc, args=(proc, 0.5)).start()
        time.sleep(0.35)
        assert proc.poll() is None, "Process should ignore SIGTERM initially"
        time.sleep(0.35)
        assert (
            proc.poll() is not None
        ), "Process should be terminated by force_terminate"
