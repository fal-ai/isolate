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

from isolate.backends.context import (
    GLOBAL_CONTEXT,
    ContextType,
    Log,
    LogLevel,
    LogSource,
)

__all__ = [
    "BasicCallable",
    "CallResultType",
    "EnvironmentConnection",
    "BaseEnvironment",
    "UserException",
    "EnvironmentCreationError",
]

ConnectionKeyType = TypeVar("ConnectionKeyType")
CallResultType = TypeVar("CallResultType")
BasicCallable = Callable[[], CallResultType]


class EnvironmentCreationError(Exception):
    """Raised when the environment cannot be created."""


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
    def from_config(
        cls,
        config: Dict[str, Any],
        context: ContextType = GLOBAL_CONTEXT,
    ) -> BaseEnvironment:
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

    def log(
        self,
        message: str,
        *,
        level: LogLevel = LogLevel.DEBUG,
        source: LogSource = LogSource.BUILDER,
    ) -> None:
        """Log a message."""
        log_msg = Log(message, level=level, source=source, bound_env=self)
        self.context.log(log_msg)


@dataclass
class UserException:
    """Represents an exception that was raised during
    the user program."""

    exception: BaseException


@dataclass
class EnvironmentConnection:
    environment: BaseEnvironment

    def __enter__(self) -> EnvironmentConnection:
        return self

    def __exit__(self, *exc_info):
        return None

    def run(
        self,
        executable: BasicCallable,
        ignore_exceptions: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> CallResultType:
        """Run the given executable inside the environment, and return the result.
        If the executable raises an exception, then it will be raised directly unless
        'ignore_exceptions' is set. In that case, the exception will be wrapped by
        a UserException box and returned."""
        raise NotImplementedError

    def log(
        self,
        message: str,
        *,
        level: LogLevel = LogLevel.TRACE,
        source: LogSource = LogSource.BRIDGE,
    ) -> None:
        """Log a message through the bound environment."""
        self.environment.log(message, level=level, source=source)
