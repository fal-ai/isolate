# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: server.proto
# Protobuf Python Version: 5.26.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from isolate.connections.grpc.definitions import common_pb2 as common__pb2
from google.protobuf import struct_pb2 as google_dot_protobuf_dot_struct__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0cserver.proto\x1a\x0c\x63ommon.proto\x1a\x1cgoogle/protobuf/struct.proto\"\x9d\x01\n\rBoundFunction\x12,\n\x0c\x65nvironments\x18\x01 \x03(\x0b\x32\x16.EnvironmentDefinition\x12#\n\x08\x66unction\x18\x02 \x01(\x0b\x32\x11.SerializedObject\x12*\n\nsetup_func\x18\x03 \x01(\x0b\x32\x11.SerializedObjectH\x00\x88\x01\x01\x42\r\n\x0b_setup_func\"d\n\x15\x45nvironmentDefinition\x12\x0c\n\x04kind\x18\x01 \x01(\t\x12.\n\rconfiguration\x18\x02 \x01(\x0b\x32\x17.google.protobuf.Struct\x12\r\n\x05\x66orce\x18\x03 \x01(\x08\x32\x37\n\x07Isolate\x12,\n\x03Run\x12\x0e.BoundFunction\x1a\x11.PartialRunResult\"\x00\x30\x01\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'server_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_BOUNDFUNCTION']._serialized_start=61
  _globals['_BOUNDFUNCTION']._serialized_end=218
  _globals['_ENVIRONMENTDEFINITION']._serialized_start=220
  _globals['_ENVIRONMENTDEFINITION']._serialized_end=320
  _globals['_ISOLATE']._serialized_start=322
  _globals['_ISOLATE']._serialized_end=377
# @@protoc_insertion_point(module_scope)
