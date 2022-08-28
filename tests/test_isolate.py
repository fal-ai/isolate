import pytest

from isolate import prepare_environment


def test_unknown_environment():
    with pytest.raises(ValueError):
        prepare_environment("$unknown_env")
