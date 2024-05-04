import pytest
from isolate.backends.settings import IsolateSettings


@pytest.fixture
def isolate_server(monkeypatch, tmp_path):
    from concurrent import futures

    import grpc
    from isolate.server import BridgeManager, IsolateServicer, definitions

    monkeypatch.setattr("isolate.server.server.INHERIT_FROM_LOCAL", True)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    test_settings = IsolateSettings(cache_dir=tmp_path / "cache")

    with BridgeManager() as bridge_manager:
        definitions.register_isolate(
            IsolateServicer(bridge_manager, test_settings), server
        )
        host, port = "localhost", server.add_insecure_port("[::]:0")
        server.start()
        try:
            yield f"{host}:{port}"
        finally:
            server.stop(None)
