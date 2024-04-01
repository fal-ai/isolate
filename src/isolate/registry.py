from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

if sys.version_info >= (3, 10):
    import importlib.metadata as importlib_metadata
else:
    import importlib_metadata

if TYPE_CHECKING:
    from isolate.backends import BaseEnvironment

# Any new environments can register themselves during package installation
# time by simply adding an entry point to the `isolate.environment` group.
_ENTRY_POINT = "isolate.backends"

_ENTRY_POINTS: dict[str, importlib_metadata.EntryPoint] = {}
_ENVIRONMENTS: dict[str, type[BaseEnvironment]] = {}


def _reload_registry() -> None:
    entry_points = importlib_metadata.entry_points()
    _ENTRY_POINTS.update(
        {
            # We are not immediately loading the backend class here
            # since it might cause importing modules that we won't be
            # using at all.
            entry_point.name: entry_point
            for entry_point in entry_points.select(group=_ENTRY_POINT)
        }
    )


_reload_registry()


def prepare_environment(
    kind: str,
    **kwargs: Any,
) -> BaseEnvironment:
    """Get the environment for the given `kind` with the given `config`."""
    from isolate.backends.settings import DEFAULT_SETTINGS

    if kind not in _ENVIRONMENTS:
        entry_point = _ENTRY_POINTS.get(kind)
        if entry_point is None:
            raise ValueError(f"Unknown environment: '{kind}'")

        _ENVIRONMENTS[kind] = entry_point.load()

    settings = kwargs.pop("context", DEFAULT_SETTINGS)
    return _ENVIRONMENTS[kind].from_config(config=kwargs, settings=settings)
