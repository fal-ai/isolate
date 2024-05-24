from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    ClassVar,
    Generic,
    Iterator,
    TypeVar,
)

from isolate.backends.settings import DEFAULT_SETTINGS, IsolateSettings
from isolate.logs import Log, LogLevel, LogSource

__all__ = [
    "BasicCallable",
    "CallResultType",
    "EnvironmentConnection",
    "BaseEnvironment",
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

    BACKEND_NAME: ClassVar[str | None] = None

    settings: IsolateSettings = DEFAULT_SETTINGS

    @classmethod
    def from_config(
        cls,
        config: dict[str, Any],
        settings: IsolateSettings = DEFAULT_SETTINGS,
    ) -> BaseEnvironment:
        """Create a new environment from the given configuration."""
        raise NotImplementedError

    @property
    def key(self) -> str:
        """A unique identifier for this environment (combination of requirements,
        python version and other relevant information) that can be used for caching
        and identification purposes."""
        raise NotImplementedError

    def create(self, *, force: bool = False) -> ConnectionKeyType:
        """Setup the given environment, and return all the information needed
        for establishing a connection to it. If `force` flag is set, then even
        if the environment is cached; it will be tried to be re-built."""
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
        """Return a new connection to the environment with using the
        `connection_key`."""
        raise NotImplementedError

    @contextmanager
    def connect(self) -> Iterator[EnvironmentConnection]:
        """Create the given environment (if it already doesn't exist) and establish a
        connection to it."""
        connection_key = self.create()
        with self.open_connection(connection_key) as connection:
            yield connection

    def apply_settings(self, settings: IsolateSettings) -> None:
        """Apply the new settings to this environment."""
        self.settings = settings

    def log(
        self,
        message: str,
        *,
        level: LogLevel = LogLevel.DEBUG,
        source: LogSource = LogSource.BUILDER,
    ) -> None:
        """Log a message."""
        log_msg = Log(message, level=level, source=source, bound_env=self)
        self.settings.log(log_msg)


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
        *args: Any,
        **kwargs: Any,
    ) -> CallResultType:  # type: ignore[type-var]
        """Run the given executable inside the environment, and return the result.
        If the executable raises an exception, then it will be raised directly."""
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
