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
    __slots__ = ["definition", "method", "was_it_raised"]
    DEFINITION_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    WAS_IT_RAISED_FIELD_NUMBER: _ClassVar[int]
    definition: bytes
    method: str
    was_it_raised: bool
    def __init__(
        self,
        method: _Optional[str] = ...,
        definition: _Optional[bytes] = ...,
        was_it_raised: bool = ...,
    ) -> None: ...

class LogSource(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class LogLevel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
