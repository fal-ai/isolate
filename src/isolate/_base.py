from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generic,
    Iterator,
    Optional,
    TypeVar,
)

from isolate.context import GLOBAL_CONTEXT, ContextType

ConnectionKeyType = TypeVar("ConnectionKeyType")
SupportedEnvironmentType = TypeVar("SupportedEnvironmentType", bound="BaseEnvironment")
CallResultType = TypeVar("CallResultType")
BasicCallable = Callable[[], CallResultType]


class BaseEnvironment(Generic[ConnectionKeyType]):
    """Represents a managed environment definition for an isolatation backend
    that can be used to run Python code with different set of dependencies."""

    BACKEND_NAME: ClassVar[Optional[str]] = None

    # Context is currently a special object that is not exposed to the users.
    # In the future there might be many contexts with different abilities (e.g.
    # a context object that changes cache location to /tmp, or a context that
    # changes the serialization form)
    context: ContextType = GLOBAL_CONTEXT

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

    def create(self) -> ConnectionKeyType:
        """Setup the given environment, and return all the information needed
        for establishing a connection to it."""
        raise NotImplementedError

    def destroy(self, connection_key: ConnectionKeyType) -> None:
        """Dismantle this environment. Might raise an exception if the environment
        does not exist."""
        raise NotImplementedError

    def exists(self) -> bool:
        """Return True if the environment already exists."""
        raise NotImplementedError

    def open_connection(
        self, connection_key: ConnectionKeyType
    ) -> EnvironmentConnection:
        """Return a new connection to the environment with using the `connection_key`."""
        raise NotImplementedError

    @contextmanager
    def connect(self) -> Iterator[EnvironmentConnection]:
        """Create the given environment (if it already doesn't exist) and establish a connection
        to it."""
        connection_key = self.create()
        with self.open_connection(connection_key) as connection:
            yield connection

    def set_context(self, context: ContextType) -> None:
        """Replace the existing context with the given `context`."""
        self.context = context

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
