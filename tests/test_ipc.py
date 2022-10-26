import operator
import sys
from dataclasses import replace
from functools import partial
from pathlib import Path

import pytest

from isolate.backends import BaseEnvironment
from isolate.backends.context import IsolateSettings
from isolate.backends.virtualenv import VirtualPythonEnvironment
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
    cloudpickle_ettings = replace(fake_env.settings, serialization_method="cloudpickle")
    fake_env.apply_settings(cloudpickle_ettings)

    with PythonIPC(fake_env, environment_path=Path(sys.prefix)) as conn:
        result = conn.run(lambda: 1 + 2)
        assert result == 3


def test_extra_inheritance_paths(tmp_path):
    first_env = VirtualPythonEnvironment(["pyjokes==0.5.0"])
    first_env.apply_settings(IsolateSettings(Path(tmp_path)))

    second_env = VirtualPythonEnvironment(["emoji==0.5.4"])
    second_env.apply_settings(IsolateSettings(Path(tmp_path)))

    with PythonIPC(
        first_env, first_env.create(), extra_inheritance_paths=[second_env.create()]
    ) as conn:
        assert conn.run(partial(eval, "__import__('pyjokes').__version__")) == "0.5.0"
        assert conn.run(partial(eval, "__import__('emoji').__version__")) == "0.5.4"

    third_env = VirtualPythonEnvironment(["pyjokes==0.6.0", "emoji==2.0.0"])
    third_env.apply_settings(IsolateSettings(Path(tmp_path)))

    with PythonIPC(
        second_env, second_env.create(), extra_inheritance_paths=[third_env.create()]
    ) as conn:
        assert conn.run(partial(eval, "__import__('pyjokes').__version__")) == "0.6.0"
        # Even if the third environment has a newer version of emoji, it won't be
        # used because since the second environment already has emoji installed and
        # it takes the precedence.
        assert conn.run(partial(eval, "__import__('emoji').__version__")) == "0.5.4"

    # Order matters, so if the first_env (with 0.5.0) is specified first then it
    # is going to take precedence.
    with PythonIPC(
        first_env, first_env.create(), extra_inheritance_paths=[third_env.create()]
    ) as conn:
        assert conn.run(partial(eval, "__import__('pyjokes').__version__")) == "0.5.0"

    with PythonIPC(
        third_env, third_env.create(), extra_inheritance_paths=[first_env.create()]
    ) as conn:
        assert conn.run(partial(eval, "__import__('pyjokes').__version__")) == "0.6.0"

    fourth_env = VirtualPythonEnvironment(["pyjokes==0.4.1", "emoji==2.1.0"])
    fourth_env.apply_settings(IsolateSettings(Path(tmp_path)))

    with PythonIPC(
        first_env,
        first_env.create(),
        extra_inheritance_paths=[third_env.create(), fourth_env.create()],
    ) as conn:
        # This comes from the first_env
        assert conn.run(partial(eval, "__import__('pyjokes').__version__")) == "0.5.0"
        # This comes from the third_env
        assert conn.run(partial(eval, "__import__('emoji').__version__")) == "2.0.0"
