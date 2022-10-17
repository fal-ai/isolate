from isolate.server.definitions import common_pb2 as _common_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import (
    ClassVar as _ClassVar,
    Mapping as _Mapping,
    Optional as _Optional,
    Union as _Union,
)

DESCRIPTOR: _descriptor.FileDescriptor

class BoundFunction(_message.Message):
    __slots__ = ["environment", "function"]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    environment: EnvironmentDefinition
    function: _common_pb2.SerializedObject
    def __init__(
        self,
        environment: _Optional[_Union[EnvironmentDefinition, _Mapping]] = ...,
        function: _Optional[_Union[_common_pb2.SerializedObject, _Mapping]] = ...,
    ) -> None: ...

class EnvironmentDefinition(_message.Message):
    __slots__ = ["configuration", "kind"]
    CONFIGURATION_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    configuration: _struct_pb2.Struct
    kind: str
    def __init__(
        self,
        kind: _Optional[str] = ...,
        configuration: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...,
    ) -> None: ...
