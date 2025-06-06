"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import collections.abc
from isolate.connections.grpc.definitions import common_pb2
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.message
import google.protobuf.struct_pb2
import sys

if sys.version_info >= (3, 8):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

@typing_extensions.final
class BoundFunction(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    ENVIRONMENTS_FIELD_NUMBER: builtins.int
    FUNCTION_FIELD_NUMBER: builtins.int
    SETUP_FUNC_FIELD_NUMBER: builtins.int
    STREAM_LOGS_FIELD_NUMBER: builtins.int
    @property
    def environments(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___EnvironmentDefinition]: ...
    @property
    def function(self) -> common_pb2.SerializedObject: ...
    @property
    def setup_func(self) -> common_pb2.SerializedObject: ...
    stream_logs: builtins.bool
    def __init__(
        self,
        *,
        environments: collections.abc.Iterable[global___EnvironmentDefinition] | None = ...,
        function: common_pb2.SerializedObject | None = ...,
        setup_func: common_pb2.SerializedObject | None = ...,
        stream_logs: builtins.bool = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_setup_func", b"_setup_func", "function", b"function", "setup_func", b"setup_func"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_setup_func", b"_setup_func", "environments", b"environments", "function", b"function", "setup_func", b"setup_func", "stream_logs", b"stream_logs"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_setup_func", b"_setup_func"]) -> typing_extensions.Literal["setup_func"] | None: ...

global___BoundFunction = BoundFunction

@typing_extensions.final
class EnvironmentDefinition(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    KIND_FIELD_NUMBER: builtins.int
    CONFIGURATION_FIELD_NUMBER: builtins.int
    FORCE_FIELD_NUMBER: builtins.int
    kind: builtins.str
    """Kind of the isolate environment."""
    @property
    def configuration(self) -> google.protobuf.struct_pb2.Struct:
        """A free-form definition of environment properties."""
    force: builtins.bool
    """Whether to force-create this environment or not."""
    def __init__(
        self,
        *,
        kind: builtins.str = ...,
        configuration: google.protobuf.struct_pb2.Struct | None = ...,
        force: builtins.bool = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["configuration", b"configuration"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["configuration", b"configuration", "force", b"force", "kind", b"kind"]) -> None: ...

global___EnvironmentDefinition = EnvironmentDefinition

@typing_extensions.final
class SubmitRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    FUNCTION_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    @property
    def function(self) -> global___BoundFunction:
        """The function to run."""
    @property
    def metadata(self) -> global___TaskMetadata:
        """Task metadata."""
    def __init__(
        self,
        *,
        function: global___BoundFunction | None = ...,
        metadata: global___TaskMetadata | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["function", b"function", "metadata", b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["function", b"function", "metadata", b"metadata"]) -> None: ...

global___SubmitRequest = SubmitRequest

@typing_extensions.final
class TaskMetadata(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    @typing_extensions.final
    class LoggerLabelsEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: builtins.str
        value: builtins.str
        def __init__(
            self,
            *,
            key: builtins.str = ...,
            value: builtins.str = ...,
        ) -> None: ...
        def ClearField(self, field_name: typing_extensions.Literal["key", b"key", "value", b"value"]) -> None: ...

    LOGGER_LABELS_FIELD_NUMBER: builtins.int
    @property
    def logger_labels(self) -> google.protobuf.internal.containers.ScalarMap[builtins.str, builtins.str]:
        """Labels to attach to the logs."""
    def __init__(
        self,
        *,
        logger_labels: collections.abc.Mapping[builtins.str, builtins.str] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["logger_labels", b"logger_labels"]) -> None: ...

global___TaskMetadata = TaskMetadata

@typing_extensions.final
class SubmitResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    TASK_ID_FIELD_NUMBER: builtins.int
    task_id: builtins.str
    def __init__(
        self,
        *,
        task_id: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["task_id", b"task_id"]) -> None: ...

global___SubmitResponse = SubmitResponse

@typing_extensions.final
class SetMetadataRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    TASK_ID_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    task_id: builtins.str
    @property
    def metadata(self) -> global___TaskMetadata: ...
    def __init__(
        self,
        *,
        task_id: builtins.str = ...,
        metadata: global___TaskMetadata | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["metadata", b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["metadata", b"metadata", "task_id", b"task_id"]) -> None: ...

global___SetMetadataRequest = SetMetadataRequest

@typing_extensions.final
class SetMetadataResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    def __init__(
        self,
    ) -> None: ...

global___SetMetadataResponse = SetMetadataResponse

@typing_extensions.final
class ListRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    def __init__(
        self,
    ) -> None: ...

global___ListRequest = ListRequest

@typing_extensions.final
class TaskInfo(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    TASK_ID_FIELD_NUMBER: builtins.int
    task_id: builtins.str
    def __init__(
        self,
        *,
        task_id: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["task_id", b"task_id"]) -> None: ...

global___TaskInfo = TaskInfo

@typing_extensions.final
class ListResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    TASKS_FIELD_NUMBER: builtins.int
    @property
    def tasks(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___TaskInfo]: ...
    def __init__(
        self,
        *,
        tasks: collections.abc.Iterable[global___TaskInfo] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["tasks", b"tasks"]) -> None: ...

global___ListResponse = ListResponse

@typing_extensions.final
class CancelRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    TASK_ID_FIELD_NUMBER: builtins.int
    task_id: builtins.str
    def __init__(
        self,
        *,
        task_id: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["task_id", b"task_id"]) -> None: ...

global___CancelRequest = CancelRequest

@typing_extensions.final
class CancelResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    def __init__(
        self,
    ) -> None: ...

global___CancelResponse = CancelResponse
