"""A common gRPC interface for both the gRPC connection implementation
and the Isolate Server to share."""

import functools
from typing import Any, Optional

from isolate.common import timestamp
from isolate.connections.common import load_serialized_object, serialize_object
from isolate.connections.grpc import definitions
from isolate.logs import Log, LogLevel, LogSource


@functools.singledispatch
def from_grpc(message: definitions.Message) -> Any:
    """Materialize a gRPC message into a Python object."""
    wrong_type = type(message).__name__
    raise NotImplementedError(f"Can't convert {wrong_type} to a Python object.")


@functools.singledispatch
def to_grpc(obj: Any) -> definitions.Message:
    """Convert a Python object into a gRPC message."""
    wrong_type = type(obj).__name__
    raise NotImplementedError(f"Cannot convert {wrong_type} to a gRPC message.")


@from_grpc.register
def _(message: definitions.SerializedObject) -> Any:
    return load_serialized_object(
        message.method,
        message.definition,
        was_it_raised=message.was_it_raised,
        stringized_traceback=message.stringized_traceback,
    )


@from_grpc.register
def _(message: definitions.Log) -> Log:
    source = LogSource(definitions.LogSource.Name(message.source).lower())
    level = LogLevel[definitions.LogLevel.Name(message.level).upper()]
    return Log(
        message=message.message,
        source=source,
        level=level,
        timestamp=timestamp.to_datetime(message.timestamp),
    )


@to_grpc.register
def _(obj: Log) -> definitions.Log:
    return definitions.Log(
        message=obj.message,
        source=definitions.LogSource.Value(obj.source.name.upper()),
        level=definitions.LogLevel.Value(obj.level.name.upper()),
        timestamp=timestamp.from_datetime(obj.timestamp),
    )


def to_serialized_object(
    obj: Any,
    method: str,
    was_it_raised: bool = False,
    stringized_traceback: Optional[str] = None,
) -> definitions.SerializedObject:
    """Convert a Python object into a gRPC message."""
    return definitions.SerializedObject(
        method=method,
        definition=serialize_object(method, obj),
        was_it_raised=was_it_raised,
        stringized_traceback=stringized_traceback,
    )
