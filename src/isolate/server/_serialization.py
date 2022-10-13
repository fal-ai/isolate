import functools
import importlib
from typing import Any, Type, TypeVar

from isolate.backends import BaseEnvironment
from isolate.backends.context import Log
from isolate.registry import prepare_environment
from isolate.server import definitions

T = TypeVar("T")


@functools.singledispatch
def from_grpc(message: definitions.Message, to: Type[T]) -> T:
    raise NotImplementedError(
        f"Cannot convert {type(message).__name__} to a Python object."
    )


@from_grpc.register
def _environment_definition(
    message: definitions.EnvironmentDefinition,
    to: Type[BaseEnvironment],
) -> BaseEnvironment:
    assert to is BaseEnvironment

    return prepare_environment(
        message.kind,
        **definitions.struct_to_dict(message.configuration),
    )


@functools.singledispatch
def to_grpc(obj: Any, to: Type[T], **kwargs) -> T:
    # Generic object serialization
    if to is definitions.SerializedObject:
        method = kwargs.pop("serialization_method")
        is_exception = kwargs.pop("is_exception")
        serialization_backend = importlib.import_module(method)
        return definitions.SerializedObject(
            definition=serialization_backend.dumps(obj),
            serialization_method=method,
            is_exception=is_exception,
        )

    raise NotImplementedError(f"Cannot convert {type(obj).__name__} to a gRPC message.")


@to_grpc.register
def _to_log(obj: Log, to: Type[definitions.Log]) -> definitions.Log:
    assert to is definitions.Log

    return definitions.Log(
        message=obj.message,
        source=getattr(definitions, obj.source.name.upper()),
        level=getattr(definitions, obj.level.name.upper()),
    )
