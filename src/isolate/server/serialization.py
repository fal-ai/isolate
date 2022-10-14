import functools
from typing import Any, Type, TypeVar

from isolate import prepare_environment
from isolate.backends import BaseEnvironment
from isolate.backends.connections.common import (
    load_serialized_object,
    serialize_object,
)
from isolate.backends.context import Log, LogLevel, LogSource
from isolate.server import definitions

T = TypeVar("T")


@functools.singledispatch
def from_grpc(message: definitions.Message, to: Type[T]) -> T:
    raise NotImplementedError(
        f"Cannot convert {type(message).__name__} to a Python object."
    )


@functools.singledispatch
def to_grpc(obj: Any, to: Type[T], **kwargs) -> T:
    # Generic object serialization
    if to is definitions.SerializedObject:
        method = kwargs.pop("method")
        was_it_raised = kwargs.pop("was_it_raised")
        return definitions.SerializedObject(
            definition=serialize_object(method, obj),
            method=method,
            was_it_raised=was_it_raised,
        )

    raise NotImplementedError(f"Cannot convert {type(obj).__name__} to a gRPC object.")


@to_grpc.register
def _to_log(obj: Log, to: Type[definitions.Log], **kwargs) -> definitions.Log:
    assert to is definitions.Log

    return definitions.Log(
        message=obj.message,
        source=getattr(definitions, obj.source.name.upper()),
        level=getattr(definitions, obj.level.name.upper()),
    )


@from_grpc.register
def _from_environment_definition(
    message: definitions.EnvironmentDefinition,
    to: Type[BaseEnvironment],
) -> BaseEnvironment:
    assert to is BaseEnvironment

    return prepare_environment(
        message.kind,
        **definitions.struct_to_dict(message.configuration),
    )


@from_grpc.register
def _from_object(obj: definitions.SerializedObject, to: Type[object]) -> Any:
    assert to is object

    return load_serialized_object(
        obj.method,
        obj.definition,
        was_it_raised=obj.was_it_raised,
    )


@from_grpc.register
def _from_log(obj: definitions.Log, to: Type[Log]) -> Log:
    assert to is Log

    return Log(
        message=obj.message,
        source=LogSource(obj.source),
        level=LogLevel(obj.level),
    )
