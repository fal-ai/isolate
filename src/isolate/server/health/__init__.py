from isolate.server.health.health_pb2 import (
    HealthCheckRequest,
    HealthCheckResponse,
)
from isolate.server.health.health_pb2_grpc import HealthServicer, HealthStub
from isolate.server.health.health_pb2_grpc import (
    add_HealthServicer_to_server as register_health,
)
