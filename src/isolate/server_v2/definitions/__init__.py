from google.protobuf.json_format import MessageToDict as struct_to_dict
from google.protobuf.message import Message
from google.protobuf.struct_pb2 import Struct

from isolate.server_v2.definitions.agent_pb2 import *
from isolate.server_v2.definitions.agent_pb2_grpc import (
    AgentServicer,
    AgentStub,
)
from isolate.server_v2.definitions.agent_pb2_grpc import (
    add_AgentServicer_to_server as register_agent,
)
from isolate.server_v2.definitions.common_pb2 import *
