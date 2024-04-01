from __future__ import annotations

import importlib
import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterator, cast

from tblib import Traceback, TracebackParseError

if TYPE_CHECKING:
    from typing import Protocol

    class SerializationBackend(Protocol):
        def loads(self, data: bytes) -> Any: ...

        def dumps(self, obj: Any) -> bytes: ...


AGENT_SIGNATURE = "IS_ISOLATE_AGENT"


@dataclass
class SerializationError(Exception):
    """An error that happened during the serialization process."""

    message: str


@contextmanager
def _step(message: str) -> Iterator[None]:
    """A context manager to capture every expression
    underneath it and if any of them fails for any reason
    then it will raise a SerializationError with the
    given message."""

    try:
        yield
    except BaseException as exception:
        raise SerializationError("Error while " + message) from exception


def as_serialization_method(backend: Any) -> SerializationBackend:
    """Ensures that the given backend has loads/dumps methods, and returns
    it as is (also convinces type checkers that the given object satisfies
    the serialization protocol)."""

    if not hasattr(backend, "loads") or not hasattr(backend, "dumps"):
        raise TypeError(
            f"The given serialization backend ({backend.__name__}) does "
            "not have one of the required methods (loads/dumps)."
        )

    return cast("SerializationBackend", backend)


def load_serialized_object(
    serialization_method: str,
    raw_object: bytes,
    *,
    was_it_raised: bool = False,
    stringized_traceback: str | None = None,
) -> Any:
    """Load the given serialized object using the given serialization method. If
    anything fails, then a SerializationError will be raised. If the was_it_raised
    flag is set to true, then the given object will be raised as an exception (instead
    of being returned)."""

    with _step(f"preparing the serialization backend ({serialization_method})"):
        serialization_backend = as_serialization_method(
            importlib.import_module(serialization_method)
        )

    with _step("deserializing the given object"):
        result = serialization_backend.loads(raw_object)

    if was_it_raised:
        raise prepare_exc(result, stringized_traceback=stringized_traceback)
    else:
        return result


def serialize_object(serialization_method: str, object: Any) -> bytes:
    """Serialize the given object using the given serialization method. If
    anything fails, then a SerializationError will be raised."""

    with _step(f"preparing the serialization backend ({serialization_method})"):
        serialization_backend = as_serialization_method(
            importlib.import_module(serialization_method)
        )

    with _step("serializing the given object"):
        return serialization_backend.dumps(object)


def is_agent() -> bool:
    """Returns true if the current process is an isolate agent."""
    return os.environ.get(AGENT_SIGNATURE) == "1"


def prepare_exc(
    exc: BaseException,
    *,
    stringized_traceback: str | None = None,
) -> BaseException:
    if stringized_traceback:
        try:
            traceback = Traceback.from_string(stringized_traceback).as_traceback()
        except TracebackParseError:
            traceback = None
    else:
        traceback = None

    exc.__traceback__ = traceback
    return exc
