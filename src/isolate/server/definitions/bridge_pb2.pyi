from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import (
    ClassVar as _ClassVar,
    Iterable as _Iterable,
    Mapping as _Mapping,
    Optional as _Optional,
    Union as _Union,
)

BRIDGE: LogSource
BUILDER: LogSource
DEBUG: LogLevel
DESCRIPTOR: _descriptor.FileDescriptor
ERROR: LogLevel
INFO: LogLevel
STDERR: LogLevel
STDOUT: LogLevel
TRACE: LogLevel
USER: LogSource
WARNING: LogLevel

class BoundFunction(_message.Message):
    __slots__ = ["environment", "function"]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    environment: EnvironmentDefinition
    function: SerializedObject
    def __init__(
        self,
        environment: _Optional[_Union[EnvironmentDefinition, _Mapping]] = ...,
        function: _Optional[_Union[SerializedObject, _Mapping]] = ...,
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

class Log(_message.Message):
    __slots__ = ["level", "message", "source"]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    level: LogLevel
    message: str
    source: LogSource
    def __init__(
        self,
        message: _Optional[str] = ...,
        source: _Optional[_Union[LogSource, str]] = ...,
        level: _Optional[_Union[LogLevel, str]] = ...,
    ) -> None: ...

class PartialRunResult(_message.Message):
    __slots__ = ["is_complete", "log", "result"]
    IS_COMPLETE_FIELD_NUMBER: _ClassVar[int]
    LOG_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    is_complete: bool
    log: _containers.RepeatedCompositeFieldContainer[Log]
    result: SerializedObject
    def __init__(
        self,
        is_complete: bool = ...,
        log: _Optional[_Iterable[_Union[Log, _Mapping]]] = ...,
        result: _Optional[_Union[SerializedObject, _Mapping]] = ...,
    ) -> None: ...

class SerializedObject(_message.Message):
    __slots__ = ["definition", "is_exception", "serialization_method"]
    DEFINITION_FIELD_NUMBER: _ClassVar[int]
    IS_EXCEPTION_FIELD_NUMBER: _ClassVar[int]
    SERIALIZATION_METHOD_FIELD_NUMBER: _ClassVar[int]
    definition: bytes
    is_exception: bool
    serialization_method: str
    def __init__(
        self,
        serialization_method: _Optional[str] = ...,
        definition: _Optional[bytes] = ...,
        is_exception: bool = ...,
    ) -> None: ...

class LogSource(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class LogLevel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
