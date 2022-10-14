import common_pb2 as _common_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import (
    ClassVar as _ClassVar,
    Mapping as _Mapping,
    Optional as _Optional,
    Union as _Union,
)

DESCRIPTOR: _descriptor.FileDescriptor

class Task(_message.Message):
    __slots__ = ["function"]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    function: _common_pb2.SerializedObject
    def __init__(
        self, function: _Optional[_Union[_common_pb2.SerializedObject, _Mapping]] = ...
    ) -> None: ...
