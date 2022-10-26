from google.protobuf.message import Message

from isolate.connections.grpc.definitions.agent_pb2 import *
from isolate.connections.grpc.definitions.agent_pb2_grpc import (
    AgentServicer,
    AgentStub,
)
from isolate.connections.grpc.definitions.agent_pb2_grpc import (
    add_AgentServicer_to_server as register_agent,
)
from isolate.connections.grpc.definitions.common_pb2 import *
