import textwrap
from concurrent import futures
from functools import partial
from pathlib import Path
from typing import Any, List, Optional, Tuple

import grpc
import pytest

from isolate.backends.context import Log, LogLevel, LogSource
from isolate.server import definitions
from isolate.server.interface import from_grpc, to_grpc, to_serialized_object
from isolate.server.server import IsolateServicer

REPO_DIR = Path(__file__).parent.parent
assert (
    REPO_DIR.exists() and REPO_DIR.name == "isolate"
), "This test should have access to isolate as an installable package."


def inherit_from_local(monkeypatch: Any, value: bool = True) -> None:
    """Enables the inherit from local mode for the isolate server."""
    monkeypatch.setattr("isolate.server.server.INHERIT_FROM_LOCAL", value)


@pytest.fixture
def stub():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    definitions.register_isolate(IsolateServicer(), server)
    host, port = "localhost", server.add_insecure_port(f"[::]:0")
    server.start()

    try:
        yield definitions.IsolateStub(grpc.insecure_channel(f"{host}:{port}"))
    finally:
        server.stop(None)


def define_environment(kind: str, **kwargs: Any) -> definitions.EnvironmentDefinition:
    struct = definitions.Struct()
    struct.update(kwargs)

    return definitions.EnvironmentDefinition(
        kind=kind,
        configuration=struct,
    )


def run_request(
    stub: definitions.IsolateStub,
    request: definitions.BoundFunction,
    *,
    build_logs: Optional[List[Log]] = None,
    bridge_logs: Optional[List[Log]] = None,
    user_logs: Optional[List[Log]] = None,
) -> definitions.SerializedObject:
    log_store = {
        LogSource.BUILDER: build_logs if build_logs is not None else [],
        LogSource.BRIDGE: bridge_logs if bridge_logs is not None else [],
        LogSource.USER: user_logs if user_logs is not None else [],
    }
    for result in stub.Run(request):
        for log in result.logs:
            log = from_grpc(log)
            log_store[log.source].append(log)

        if result.is_complete:
            return result.result


@pytest.mark.parametrize("inherit_local", [True, False])
def test_server_basic_communication(
    stub: definitions.IsolateStub,
    monkeypatch: Any,
    inherit_local: bool,
) -> None:
    inherit_from_local(monkeypatch, inherit_local)
    requirements = ["pyjokes==0.6.0"]
    if not inherit_local:
        # The agent process needs dill (and isolate) to actually
        # deserialize the given function, so they need to be installed
        # when we are not inheriting the local environment.
        requirements.append("dill==0.3.5.1")

        # TODO: apparently [server] doesn't work but [grpc] does work (not sure why
        # needs further investigation, probably poetry related).
        requirements.append(f"{REPO_DIR}[grpc]")

    env_definition = define_environment("virtualenv", requirements=requirements)
    request = definitions.BoundFunction(
        function=to_serialized_object(
            partial(
                eval,
                "__import__('pyjokes').__version__",
            ),
            method="dill",
        ),
        environment=env_definition,
    )

    raw_result = run_request(stub, request)
    assert from_grpc(raw_result) == "0.6.0"


def test_server_builder_error(stub: definitions.IsolateStub, monkeypatch: Any) -> None:
    inherit_from_local(monkeypatch)

    # $$$$ as a package can't exist on PyPI since PEP 508 explicitly defines
    # what is considered to be a legit package name.
    #
    # https://peps.python.org/pep-0508/#names

    env_definition = define_environment("virtualenv", requirements=["$$$$"])
    request = definitions.BoundFunction(
        function=to_serialized_object(
            partial(
                eval,
                "__import__('pyjokes').__version__",
            ),
            method="dill",
        ),
        environment=env_definition,
    )

    build_logs: List[Log] = []
    with pytest.raises(grpc.RpcError) as exc:
        run_request(stub, request, build_logs=build_logs)

    assert exc.match("A problem occurred while creating the environment")

    raw_logs = [log.message for log in build_logs]
    assert "ERROR: Invalid requirement: '$$$$'" in raw_logs


def test_user_logs_immediate(stub: definitions.IsolateStub, monkeypatch: Any) -> None:
    inherit_from_local(monkeypatch)

    env_definition = define_environment("virtualenv", requirements=["pyjokes==0.6.0"])
    request = definitions.BoundFunction(
        function=to_serialized_object(
            partial(
                exec,
                textwrap.dedent(
                    """
                import sys, pyjokes, time
                print(pyjokes.__version__)
                time.sleep(0.1)
                print("error error!", file=sys.stderr)
                time.sleep(0.1)
                """
                ),
            ),
            method="dill",
        ),
        environment=env_definition,
    )

    user_logs: List[Log] = []
    run_request(stub, request, user_logs=user_logs)

    assert len(user_logs) == 2

    by_stream = {log.level: log.message for log in user_logs}
    assert by_stream[LogLevel.STDOUT] == "0.6.0"
    assert by_stream[LogLevel.STDERR] == "error error!"
