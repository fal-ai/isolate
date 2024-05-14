from __future__ import annotations

import importlib
import os
from typing import TYPE_CHECKING, Any, cast

from tblib import Traceback, TracebackParseError

from isolate.exceptions import IsolateException

if TYPE_CHECKING:
    from typing import Protocol

    class SerializationBackend(Protocol):
        def loads(self, data: bytes) -> Any: ...

        def dumps(self, obj: Any) -> bytes: ...


AGENT_SIGNATURE = "IS_ISOLATE_AGENT"


class BaseSerializationError(IsolateException):
    """An error that happened during the serialization process."""

    pass


class SerializationError(BaseSerializationError):
    pass


class DeserializationError(BaseSerializationError):
    pass


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

    try:
        serialization_backend = as_serialization_method(
            importlib.import_module(serialization_method)
        )
    except BaseException as exc:
        raise DeserializationError(
            "Error while preparing the serialization backend "
            f"({serialization_method})"
        ) from exc

    try:
        result = serialization_backend.loads(raw_object)
    except BaseException as exc:
        raise DeserializationError(
            "Error while deserializing the given object"
        ) from exc

    if was_it_raised:
        raise prepare_exc(result, stringized_traceback=stringized_traceback)
    else:
        return result


def serialize_object(serialization_method: str, object: Any) -> bytes:
    """Serialize the given object using the given serialization method. If
    anything fails, then a SerializationError will be raised."""

    try:
        serialization_backend = as_serialization_method(
            importlib.import_module(serialization_method)
        )
    except BaseException as exc:
        raise SerializationError(
            f"Error while preparing the serialization backend ({serialization_method})"
        ) from exc

    try:
        return serialization_backend.dumps(object)
    except BaseException as exc:
        raise SerializationError("Error while serializing the given object") from exc


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
