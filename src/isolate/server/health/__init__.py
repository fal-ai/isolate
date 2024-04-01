from isolate.server.health.health_pb2 import (  # noqa: F401
    HealthCheckRequest,
    HealthCheckResponse,
)
from isolate.server.health.health_pb2_grpc import HealthServicer, HealthStub  # noqa: F401
from isolate.server.health.health_pb2_grpc import (  # noqa: F401
    add_HealthServicer_to_server as register_health,
)
