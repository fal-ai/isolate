from __future__ import annotations

import importlib
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from functools import lru_cache, partial
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    cast,
)

import importlib_metadata
import rich
from rich.console import Console
from rich.status import Status
from rich.text import Text

import isolate
from isolate.backends import BaseEnvironment
from isolate.backends.settings import IsolateSettings
from isolate.logs import Log, LogLevel, LogSource

ReturnType = TypeVar("ReturnType")

# Whether to enable debug logging or not.
_DEBUG_LOGGING = bool(os.environ.get("ISOLATE_ENABLE_DEBUG_LOGGING", False))

# List of serialization method options. Ordered by their
# priority/preference.
_SERIALIZATION_OPTIONS = ["dill", "cloudpickle"]


@lru_cache(1)
def _decide_default_backend():
    for option in _SERIALIZATION_OPTIONS:
        try:
            importlib.import_module(option)
        except ImportError:
            continue
        else:
            return option

    rich.print(
        "Falling back to the default serialization method: 'pickle'.\n"
        "For the best experience, please install one of the following: "
        f"{', '.join(map(repr, _SERIALIZATION_OPTIONS))}"
    )
    return "pickle"


def Environment(kind: str, **config: Any) -> _EnvironmentBuilder:
    default_pkgs = []

    default_backend = _decide_default_backend()
    if default_backend != "pickle":
        default_pkgs.append(
            (default_backend, importlib_metadata.version(default_backend))
        )

    if kind == "virtualenv":
        requirements = config.setdefault("requirements", [])
        requirements.extend(f"{name}=={version}" for name, version in default_pkgs)
        return _PackageCollector(kind, config, collect_into=requirements)
    elif kind == "conda":
        packages = config.setdefault("packages", [])
        packages.extend(f"{name}={version}" for name, version in default_pkgs)
        return _PackageCollector(kind, config, collect_into=packages)
    else:
        raise NotImplementedError(f"Unknown environment kind: {kind}")


@dataclass
class _EnvironmentBuilder:
    def get_definition(self) -> Tuple[Dict[str, Any], IsolateSettings]:
        """Return the isolate definition for this environment!"""
        raise NotImplementedError

    def __rshift__(self, left: Any) -> BoxedEnvironment:
        """Put an environment into a box."""
        if not isinstance(left, Box):
            return NotImplemented

        definition, settings = self.get_definition()
        return left.wrap_it(definition, settings)


@dataclass(repr=False)
class _PackageCollector(_EnvironmentBuilder):
    kind: str
    config_options: Dict[str, Any]
    collect_into: List[str]

    def __lshift__(self, right: Any) -> Any:
        if not isinstance(right, str):
            return NotImplemented

        self.collect_into.append(right)
        return self

    def get_definition(self) -> Tuple[Dict[str, Any], IsolateSettings]:
        settings = IsolateSettings(serialization_method=_decide_default_backend())
        return {
            "kind": self.kind,
            **self.config_options,
        }, settings

    def __repr__(self):
        return f"{self.kind}({', '.join(f'{k}={v!r}' for k, v in self.config_options.items())})"


@dataclass
class Box:
    """Some sort of a box."""

    def wrap_it(
        self,
        definition: Dict[str, Any],
        settings: IsolateSettings,
    ) -> BoxedEnvironment:
        raise NotImplementedError

    replace = replace


@dataclass
class LocalBox(Box):
    """Run locally."""

    parallelism: int = 1

    def wrap_it(
        self,
        definition: Dict[str, Any],
        settings: IsolateSettings,
    ) -> BoxedEnvironment:
        return BoxedEnvironment(
            isolate.prepare_environment(
                **definition,
                context=settings,
            ),
            parallelism=self.parallelism,
        )

    def __mul__(self, right: int) -> LocalBox:
        if not isinstance(right, int):
            return NotImplemented

        return self.replace(parallelism=self.parallelism * right)


@dataclass
class RemoteBox(Box):
    """Run on an hosted isolate server."""

    host: str
    parallelism: int = 1

    def wrap_it(
        self,
        definition: Dict[str, Any],
        settings: IsolateSettings,
    ) -> BoxedEnvironment:
        definition = definition.copy()

        # Extract the kind of environment to use.
        kind = definition.pop("kind", None)
        assert kind is not None, f"Corrupted definition: {definition}"

        # Create a remote environment.
        return BoxedEnvironment(
            isolate.prepare_environment(
                "isolate-server",
                host=self.host,
                target_environment_kind=kind,
                target_environment_config=definition,
                context=settings,
            ),
            parallelism=self.parallelism,
        )

    def __mul__(self, right: int) -> RemoteBox:
        if not isinstance(right, int):
            return NotImplemented

        return self.replace(parallelism=self.parallelism * right)


@dataclass
class BoxedEnvironment:
    """Environment-in-a-box! A user friendly wrapper around the isolate
    environments!"""

    environment: BaseEnvironment
    parallelism: int = 1
    _console: Console = field(
        default_factory=partial(Console, highlighter=None), repr=False
    )
    _is_building: bool = field(default=True, repr=False)
    _status: Optional[Status] = field(default=None, repr=False)
    _active_parallelism: Optional[str] = field(default=None, repr=False)

    def __post_init__(self):
        existing_settings = self.environment.settings
        new_settings = existing_settings.replace(log_hook=self._rich_log)
        self.environment.apply_settings(new_settings)

    def _update_status(self, from_builder: bool = False) -> None:
        if self._status is not None:
            if from_builder:
                self._status.update("Building the environment...", spinner="clock")
            else:
                if self._active_parallelism:
                    self._status.update(
                        f"Running the isolated tasks {self._active_parallelism}",
                        spinner="runner",
                    )
                else:
                    self._status.update("Running the isolated task", spinner="runner")

    def _rich_log(self, log: Log) -> None:
        self._update_status(from_builder=log.source is LogSource.BUILDER)
        if log.source is LogSource.USER:
            # If the log is originating from user code, then print it
            # as a normal message.
            self._console.print(log.message)
        else:
            # Otherwise sprinkle some colors on it!
            allowed_levels = {
                LogLevel.ERROR: "red",
                LogLevel.WARNING: "yellow",
                LogLevel.INFO: "white",
            }
            if _DEBUG_LOGGING or log.source is LogSource.BUILDER:
                allowed_levels[LogLevel.DEBUG] = "blue"
                allowed_levels[LogLevel.TRACE] = "grey"

            if log.level in allowed_levels:
                level = Text(f"[{log.source}]", style=allowed_levels[log.level])
                self._console.print(level + " " + log.message)

    @contextmanager
    def _status_display(self, message: str) -> Iterator[None]:
        assert self._status is None

        try:
            self._status = self._console.status(message)
            with self._status:
                yield
        finally:
            self._status = None
            self._active_parallelism = None

    def run(
        self,
        func: Callable[..., ReturnType],
        *args: Any,
        **kwargs: Any,
    ) -> ReturnType:
        """Run the given `func` in the environment with the passed arguments."""
        executable = partial(func, *args, **kwargs)
        with self._status_display("Preparing for execution..."):
            with self.environment.connect() as connection:
                return cast(ReturnType, connection.run(executable))

    def map(
        self,
        func: Callable[..., ReturnType],
        *iterables: Iterable[Any],
    ) -> Iterable[ReturnType]:
        with self._status_display("Preparing for execution..."):
            with ThreadPoolExecutor(max_workers=self.parallelism) as executor:
                with self.environment.connect() as connection:
                    futures = [
                        executor.submit(connection.run, partial(func, *args))
                        for args in zip(*iterables)
                    ]
                    self._active_parallelism = f"0/{len(futures)}"
                    for n, future in enumerate(as_completed(futures), 1):
                        yield cast(ReturnType, future.result())
                        self._active_parallelism = f"{n}/{len(futures)}"
                        self._update_status()
