from pathlib import Path

import pytest

from isolate.backends.settings import IsolateSettings


@pytest.fixture(scope="session")
def isolate_cache_dir() -> Path:
    cache_dir = Path(__file__).parent / "isolate-cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


@pytest.fixture
def isolate_settings(isolate_cache_dir: Path) -> IsolateSettings:
    return IsolateSettings(cache_dir=isolate_cache_dir)


@pytest.fixture
def isolate_server(monkeypatch, tmp_path, isolate_settings):
    from concurrent import futures

    import grpc

    from isolate.server import BridgeManager, IsolateServicer, definitions

    monkeypatch.setattr("isolate.server.server.INHERIT_FROM_LOCAL", True)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    test_settings = isolate_settings

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
