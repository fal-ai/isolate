import operator
import sys
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
