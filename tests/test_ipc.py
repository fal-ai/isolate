import operator
import sys
from dataclasses import replace
from functools import partial
from pathlib import Path

import pytest

from isolate.backends import BaseEnvironment
from isolate.backends.connections import DualPythonIPC, PythonIPC
from isolate.backends.context import _Context
from isolate.backends.virtual_env import VirtualPythonEnvironment


class FakeEnvironment(BaseEnvironment):
    @property
    def key(self) -> str:
        return "<test>"


def test_python_ipc():
    fake_env = FakeEnvironment()

    # This is going to create a new Python process by using
    # the same Python as the one that is running the test. And
    # then it will execute some arbitrary code.
    with PythonIPC(fake_env, environment_path=Path(sys.prefix)) as conn:
        result = conn.run(partial(operator.add, 1, 2))
        assert result == 3


def test_python_ipc_serialization():
    fake_env = FakeEnvironment()

    # By default the serialization uses pickle, which can't serialize
    # an anonymous function.
    with PythonIPC(fake_env, environment_path=Path(sys.prefix)) as conn:
        with pytest.raises(Exception):
            result = conn.run(lambda: 1 + 2)

    # But we can switch serialization backends, and use cloudpickle
    # which can serialize anonymous functions.
    cloudpickle_context = replace(
        fake_env.context, _serialization_backend="cloudpickle"
    )
    fake_env.set_context(cloudpickle_context)

    with PythonIPC(fake_env, environment_path=Path(sys.prefix)) as conn:
        result = conn.run(lambda: 1 + 2)
        assert result == 3


def test_dual_python_ipc(tmp_path):
    first_env = VirtualPythonEnvironment(["pyjokes==0.5.0"])
    first_env.set_context(_Context(Path(tmp_path)))

    second_env = VirtualPythonEnvironment(["emoji==0.5.4"])
    second_env.set_context(_Context(Path(tmp_path)))

    with DualPythonIPC(first_env, first_env.create(), second_env.create()) as conn:
        assert conn.run(partial(eval, "__import__('pyjokes').__version__")) == "0.5.0"
        assert conn.run(partial(eval, "__import__('emoji').__version__")) == "0.5.4"

    third_env = VirtualPythonEnvironment(["pyjokes==0.6.0"])
    third_env.set_context(_Context(Path(tmp_path)))

    with DualPythonIPC(first_env, second_env.create(), third_env.create()) as conn:
        assert conn.run(partial(eval, "__import__('pyjokes').__version__")) == "0.6.0"
        assert conn.run(partial(eval, "__import__('emoji').__version__")) == "0.5.4"

    # Order matters, so if the first_env (with 0.5.0) is specified first then it
    # is going to take precedence.
    with DualPythonIPC(first_env, first_env.create(), third_env.create()) as conn:
        assert conn.run(partial(eval, "__import__('pyjokes').__version__")) == "0.5.0"

    with DualPythonIPC(first_env, third_env.create(), first_env.create()) as conn:
        assert conn.run(partial(eval, "__import__('pyjokes').__version__")) == "0.6.0"
