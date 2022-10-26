from isolate import prepare_environment
from isolate.backends import BaseEnvironment
from isolate.connections.grpc.interface import (
    from_grpc,
    to_grpc,
    to_serialized_object,
)
from isolate.server import definitions

__all__ = ["from_grpc", "to_grpc", "to_serialized_object"]


@from_grpc.register
def _(message: definitions.EnvironmentDefinition) -> BaseEnvironment:
    return prepare_environment(
        message.kind,
        **definitions.struct_to_dict(message.configuration),
    )
