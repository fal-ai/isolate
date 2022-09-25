from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, Any, Dict

from isolate.backends.context import GLOBAL_CONTEXT
from isolate.registry import prepare_environment

if TYPE_CHECKING:
    from isolate.backends import BaseEnvironment


class EnvironmentManager:
    """A managed environment provider that handles the creation of
    environments, provisioning of agents, and the execution of tasks."""

    def __init__(self):
        self._environments: Dict[str, BaseEnvironment] = {}
        self._manager_context = GLOBAL_CONTEXT._replace(
            _serialization_backend="cloudpickle"
        )

    def register(
        self,
        name: str,
        kind: str,
        **kwargs: Any,
    ) -> None:
        """Register a new environment with the given name."""
        if kind == "conda":
            kwargs.setdefault("packages", []).append("cloudpickle=2.1.0")
        elif kind == "virtualenv":
            kwargs.setdefault("requirements", []).append("cloudpickle==2.1.0")
        else:
            raise ValueError(f"Unsupported isolation backend for the manager: '{kind}'")

        self._environments[name] = prepare_environment(
            kind=kind,
            **kwargs,
            context=self._manager_context,
        )

    def run(
        self,
        name: str,
        func: Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Run the provided function in the given environment."""
        environment = self._environments.get(name)
        if not environment:
            raise ValueError(f"Unknown environment: '{name}'")

        basic_callable = partial(func, *args, **kwargs)
        with environment.connect() as connection:
            return connection.run(basic_callable)
