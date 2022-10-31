from typing import Any, Dict

from isolate.backends import BaseEnvironment
from isolate.connections.grpc.interface import (
    from_grpc,
    to_grpc,
    to_serialized_object,
)
from isolate.server import definitions

__all__ = ["from_grpc", "to_grpc", "to_serialized_object", "to_struct"]


@from_grpc.register
def _(message: definitions.EnvironmentDefinition) -> BaseEnvironment:
    from isolate import prepare_environment

    return prepare_environment(
        message.kind,
        **definitions.struct_to_dict(message.configuration),
    )


def to_struct(data: Dict[str, Any]) -> definitions.Struct:
    struct = definitions.Struct()
    struct.update(data)
    return struct
