# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: common.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x0c\x63ommon.proto"M\n\x10SerializedObject\x12\x0e\n\x06method\x18\x01 \x01(\t\x12\x12\n\ndefinition\x18\x02 \x01(\x0c\x12\x15\n\rwas_it_raised\x18\x03 \x01(\x08"n\n\x10PartialRunResult\x12\x13\n\x0bis_complete\x18\x01 \x01(\x08\x12\x12\n\x04logs\x18\x02 \x03(\x0b\x32\x04.Log\x12&\n\x06result\x18\x03 \x01(\x0b\x32\x11.SerializedObjectH\x00\x88\x01\x01\x42\t\n\x07_result"L\n\x03Log\x12\x0f\n\x07message\x18\x01 \x01(\t\x12\x1a\n\x06source\x18\x02 \x01(\x0e\x32\n.LogSource\x12\x18\n\x05level\x18\x03 \x01(\x0e\x32\t.LogLevel*.\n\tLogSource\x12\x0b\n\x07\x42UILDER\x10\x00\x12\n\n\x06\x42RIDGE\x10\x01\x12\x08\n\x04USER\x10\x02*Z\n\x08LogLevel\x12\t\n\x05TRACE\x10\x00\x12\t\n\x05\x44\x45\x42UG\x10\x01\x12\x08\n\x04INFO\x10\x02\x12\x0b\n\x07WARNING\x10\x03\x12\t\n\x05\x45RROR\x10\x04\x12\n\n\x06STDOUT\x10\x05\x12\n\n\x06STDERR\x10\x06\x62\x06proto3'
)

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "common_pb2", globals())
if _descriptor._USE_C_DESCRIPTORS == False:

    DESCRIPTOR._options = None
    _LOGSOURCE._serialized_start = 285
    _LOGSOURCE._serialized_end = 331
    _LOGLEVEL._serialized_start = 333
    _LOGLEVEL._serialized_end = 423
    _SERIALIZEDOBJECT._serialized_start = 16
    _SERIALIZEDOBJECT._serialized_end = 93
    _PARTIALRUNRESULT._serialized_start = 95
    _PARTIALRUNRESULT._serialized_end = 205
    _LOG._serialized_start = 207
    _LOG._serialized_end = 283
# @@protoc_insertion_point(module_scope)