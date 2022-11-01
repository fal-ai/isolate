from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Type, Union

import importlib_metadata

if TYPE_CHECKING:
    from isolate.backends import BaseEnvironment

# Any new environments can register themselves during package installation
# time by simply adding an entry point to the `isolate.environment` group.
_ENTRY_POINT = "isolate.backends"


_ENVIRONMENT_REGISTRY: Dict[
    str, Union[importlib_metadata.EntryPoint, Type["BaseEnvironment"]]
] = {}


def _reload_registry() -> None:
    entry_points = importlib_metadata.entry_points()
    _ENVIRONMENT_REGISTRY.update(
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

    registered_env_cls = _ENVIRONMENT_REGISTRY.get(kind)
    if not registered_env_cls:
        raise ValueError(f"Unknown environment: '{kind}'")

    if isinstance(registered_env_cls, importlib_metadata.EntryPoint):
        _ENVIRONMENT_REGISTRY[kind] = registered_env_cls = registered_env_cls.load()

    settings = kwargs.pop("context", DEFAULT_SETTINGS)
    return registered_env_cls.from_config(config=kwargs, settings=settings)
