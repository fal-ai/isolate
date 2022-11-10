from typing import Any, Dict

from isolate.backends import BaseEnvironment
from isolate.connections.grpc.interface import (
    from_grpc,
    to_grpc,
    to_serialized_object,
)
from isolate.server import definitions
from google._upb._message import RepeatedCompositeContainer

__all__ = ["from_grpc", "to_grpc", "to_serialized_object", "to_struct"]


@from_grpc.register
def _(message: definitions.EnvironmentDefinition) -> BaseEnvironment:
    from isolate import prepare_environment

    return prepare_environment(
        message.kind,
        **definitions.struct_to_dict(message.configuration),
    )

@from_grpc.register
def _(messages: RepeatedCompositeContainer) -> Any:
    # TODO: Right now this function is called for every list, it should only be called on env definitions
    from isolate import prepare_environment
    envs = []
    for message in messages:
        assert type(message) is definitions.EnvironmentDefinition, f"Unexpected message type: {type(message)}"
        envs.append(prepare_environment(
            message.kind,
            **definitions.struct_to_dict(message.configuration),
        ))
    return envs

def to_struct(data: Dict[str, Any]) -> definitions.Struct:
    struct = definitions.Struct()
    struct.update(data)
    return struct
