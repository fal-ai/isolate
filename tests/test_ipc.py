import operator
import sys
import pytest
from dataclasses import replace
from functools import partial
from pathlib import Path

from isolate import BaseEnvironment
from isolate.connections import PythonIPC


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
    cloudpickle_context = replace(fake_env.context, _serialization_backend="cloudpickle")
    fake_env.set_context(cloudpickle_context)

    with PythonIPC(fake_env, environment_path=Path(sys.prefix)) as conn:
        result = conn.run(lambda: 1 + 2)
        assert result == 3
