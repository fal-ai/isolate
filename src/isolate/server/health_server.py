import asyncio
from dataclasses import dataclass
from typing import AsyncIterator

from grpc.aio import ServicerContext

from isolate.server import health


@dataclass
class HealthServicer(health.HealthServicer):
    def __post_init__(self):
        self._state = {
            # Empty refers to the whole server
            "": health.HealthCheckResponse.ServingStatus.SERVING,
            "isolate": health.HealthCheckResponse.ServingStatus.SERVING,
        }

    def _get_status(
        self, service: str
    ) -> health.HealthCheckResponse.ServingStatus.ValueType:
        status = self._state.get(
            service,
            health.HealthCheckResponse.ServingStatus.SERVICE_UNKNOWN,
        )
        return status

    def Check(
        self, request: health.HealthCheckRequest, context: ServicerContext
    ) -> health.HealthCheckResponse:
        return health.HealthCheckResponse(status=self._get_status(request.service))

    async def Watch(
        self,
        request: health.HealthCheckRequest,
        context: ServicerContext,
    ) -> AsyncIterator[health.HealthCheckResponse]:
        while True:
            yield health.HealthCheckResponse(status=self._get_status(request.service))
            await asyncio.sleep(2)
