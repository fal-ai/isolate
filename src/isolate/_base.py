from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    Optional,
    Type,
    TypeVar,
)

ConnectionKeyType = TypeVar("ConnectionKeyType")
SupportedEnvironmentType = TypeVar("SupportedEnvironmentType", bound="BaseEnvironment")
CallResultType = TypeVar("CallResultType")
BasicCallable = Callable[[], CallResultType]


class BaseEnvironment(Generic[ConnectionKeyType]):
    """Represents a managed environment definition for an isolatation backend
    that can be used to run Python code with different set of dependencies."""

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> BaseEnvironment:
        """Create a new environment from the given configuration."""
        raise NotImplementedError

    @property
    def key(self) -> str:
        """A unique identifier for this environment (combination of requirements,
        python version and other relevant information) that can be used for caching
        and identification purposes."""
        raise NotImplementedError

    def create(self, *, exist_ok: bool = False) -> ConnectionKeyType:
        """Setup the given environment, and return all the information needed
        for establishing a connection to it. If the environment already exists,
        raise an exception unless `exist_ok` is True."""
        raise NotImplementedError

    def destroy(self, conn_info: ConnectionKeyType) -> None:
        """Dismantle this environment. Raises an exception if the environment
        does not exist."""
        raise NotImplementedError

    def open_connection(self, conn_info: ConnectionKeyType) -> EnvironmentConnection:
        """Return a new connection to the environment residing inside
        given path."""
        raise NotImplementedError

    @contextmanager
    def connect(self) -> Iterator[EnvironmentConnection]:
        """Create the given environment (if it already doesn't exist) and establish a connection
        to it."""
        connection_key = self.create(exist_ok=True)
        with self.open_connection(connection_key) as connection:
            yield connection

    def log(self, message: str, *args: Any, kind: str = "trace", **kwargs: Any) -> None:
        """Log a message."""
        print(f"[{kind}] [{self.key}] {message.format(*args, **kwargs)}")


@dataclass
class EnvironmentConnection:
    environment: BaseEnvironment

    def __enter__(self) -> EnvironmentConnection:
        return self

    def __exit__(self, *exc_info):
        return None

    def run(
        self, executable: BasicCallable, *args: Any, **kwargs: Any
    ) -> CallResultType:
        """Run the given executable inside the environment, and return the result."""
        raise NotImplementedError

    def log(self, message: str, *args: Any, kind: str = "trace", **kwargs: Any) -> None:
        """Log a message through the bound environment."""
        self.environment.log("[connection] " + message, *args, **kwargs)
