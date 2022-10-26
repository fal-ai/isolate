from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Type

import importlib_metadata

if TYPE_CHECKING:
    from isolate.backends import BaseEnvironment

# Any new environments can register themselves during package installation
# time by simply adding an entry point to the `isolate.environment` group.
_ENTRY_POINT = "isolate.backends"


_ENVIRONMENT_REGISTRY: Dict[str, Type["BaseEnvironment"]] = {}


def _reload_registry() -> None:
    entry_points = importlib_metadata.entry_points()
    _ENVIRONMENT_REGISTRY.update(
        {
            entry_point.name: entry_point.load()
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

    settings = kwargs.pop("context", DEFAULT_SETTINGS)
    return registered_env_cls.from_config(config=kwargs, settings=settings)
