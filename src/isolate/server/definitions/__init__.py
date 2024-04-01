from google.protobuf.json_format import MessageToDict as struct_to_dict  # noqa: F401
from google.protobuf.struct_pb2 import Struct  # noqa: F401

# Inherit everything from the gRPC connection handler.
from isolate.connections.grpc.definitions import *  # noqa: F403
from isolate.server.definitions.server_pb2 import *  # noqa: F403
from isolate.server.definitions.server_pb2_grpc import (  # noqa: F401
    IsolateServicer,
    IsolateStub,
)
from isolate.server.definitions.server_pb2_grpc import (  # noqa: F401
    add_IsolateServicer_to_server as register_isolate,
)
