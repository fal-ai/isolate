from google.protobuf.json_format import MessageToDict as struct_to_dict
from google.protobuf.struct_pb2 import Struct

# Inherit everything from the gRPC connection handler.
from isolate.connections.grpc.definitions import *
from isolate.server.definitions.server_pb2 import *
from isolate.server.definitions.server_pb2_grpc import (
    IsolateServicer,
    IsolateStub,
)
from isolate.server.definitions.server_pb2_grpc import (
    add_IsolateServicer_to_server as register_isolate,
)
