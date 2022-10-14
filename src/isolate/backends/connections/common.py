from __future__ import annotations

import importlib
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Iterator, cast

if TYPE_CHECKING:
    from typing import Protocol

    class SerializationBackend(Protocol):
        def loads(self, data: bytes) -> Any:
            ...

        def dumps(self, obj: Any) -> bytes:
            ...


class SerializationError(Exception):
    """An error that happened during the serialization process."""


@contextmanager
def _step(message: str) -> Iterator[None]:
    """A context manager to capture every expression
    underneath it and if any of them fails for any reason
    then it will raise a SerializationError with the
    given message."""

    try:
        yield
    except BaseException as exception:
        raise SerializationError(message) from exception


def as_serialization_backend(backend: Any) -> SerializationBackend:
    """Ensures that the given backend has loads/dumps methods, and returns
    it as is (also convinces type checkers that the given object satisfies
    the serialization protocol)."""

    if not hasattr(backend, "loads") or not hasattr(backend, "dumps"):
        raise TypeError(
            f"The given serialization backend ({backend.__name__}) does "
            "not have one of the required methods (loads/dumps)."
        )

    return cast(SerializationBackend, backend)


def load_serialized_object(serialization_method: str, raw_object: bytes) -> Any:
    """Load the given serialized object using the given serialization method. If
    anything fails, then a SerializationError will be raised."""

    with _step(f"Preparing the serialization backend ({serialization_method})"):
        serialization_backend = as_serialization_backend(
            importlib.import_module(serialization_method)
        )

    with _step("Deserializing the given object"):
        return serialization_backend.loads(raw_object)


def serialize_object(serialization_method: str, object: Any) -> bytes:
    """Serialize the given object using the given serialization method. If
    anything fails, then a SerializationError will be raised."""

    with _step(f"Preparing the serialization backend ({serialization_method})"):
        serialization_backend = as_serialization_backend(
            importlib.import_module(serialization_method)
        )

    with _step("Deserializing the given object"):
        return serialization_backend.dumps(object)
