from unittest.mock import create_autospec, patch

import pytest

from insulate import prepare_environment


@pytest.fixture
def fresh_registry(monkeypatch):
    """Temporarily clear the environment registry for this test. Also restores
    back to the initial state once the test is executed."""
    monkeypatch.setattr("insulate.registry._ENVIRONMENT_REGISTRY", {})


def test_unknown_environment(fresh_registry):
    with pytest.raises(ValueError):
        prepare_environment("$unknown_env")


def test_environment_discovery(fresh_registry):
    # This test currently depends on too-much internals, but
    # can be improved later on.

    from importlib_metadata import EntryPoint, EntryPoints

    from insulate.registry import _ENTRY_POINT, _reload_registry

    fake_ep = create_autospec(
        EntryPoint,
    )
    fake_ep.name = "fake"
    fake_ep.value = "insulate.backends._base.BaseEnvironment"
    fake_ep.group = "insulate.environments"

    with patch("importlib_metadata.entry_points", return_value=EntryPoints([fake_ep])):
        _reload_registry()

        environment = prepare_environment("fake")
