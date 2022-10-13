from google.protobuf.json_format import MessageToDict as struct_to_dict
from google.protobuf.message import Message
from google.protobuf.struct_pb2 import Struct

from isolate.server.definitions.bridge_pb2 import *
from isolate.server.definitions.bridge_pb2_grpc import (
    BridgeServicer,
    BridgeStub,
)
from isolate.server.definitions.bridge_pb2_grpc import (
    add_BridgeServicer_to_server as register_servicer,
)
