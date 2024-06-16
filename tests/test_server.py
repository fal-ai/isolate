import copy
import textwrap
from concurrent import futures
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, List, Optional, cast

import grpc
import pytest
from isolate.backends.settings import IsolateSettings
from isolate.connections.grpc.configuration import get_default_options
from isolate.logs import Log, LogLevel, LogSource
from isolate.server import definitions, health
from isolate.server.health_server import HealthServicer
from isolate.server.interface import from_grpc, to_serialized_object
from isolate.server.server import BridgeManager, IsolateServicer

REPO_DIR = Path(__file__).parent.parent
assert (
    REPO_DIR.exists() and REPO_DIR.name == "isolate"
), "This test should have access to isolate as an installable package."


def inherit_from_local(monkeypatch: Any, value: bool = True) -> None:
    """Enables the inherit from local mode for the isolate server."""
    monkeypatch.setattr("isolate.server.server.INHERIT_FROM_LOCAL", value)


@dataclass
class Stubs:
    isolate_stub: definitions.IsolateStub
    health_stub: health.HealthStub


@contextmanager
def make_server(tmp_path):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=1), options=get_default_options()
    )
    test_settings = IsolateSettings(cache_dir=tmp_path / "cache")
    with BridgeManager() as bridge:
        servicer = IsolateServicer(bridge, test_settings)
        definitions.register_isolate(servicer, server)
        health.register_health(HealthServicer(), server)
        host, port = "localhost", server.add_insecure_port("[::]:0")
        server.start()

        try:
            isolate_stub = definitions.IsolateStub(
                grpc.insecure_channel(
                    f"{host}:{port}",
                    options=get_default_options(),
                )
            )

            health_stub = health.HealthStub(
                grpc.insecure_channel(
                    f"{host}:{port}",
                    options=get_default_options(),
                )
            )

            yield Stubs(isolate_stub=isolate_stub, health_stub=health_stub)
        finally:
            server.stop(None)
            servicer.cancel_tasks()


@pytest.fixture
def stub(tmp_path):
    with make_server(tmp_path) as stubs:
        yield stubs.isolate_stub


@pytest.fixture
def health_stub(tmp_path):
    with make_server(tmp_path) as stubs:
        yield stubs.health_stub


def define_environment(kind: str, **kwargs: Any) -> definitions.EnvironmentDefinition:
    struct = definitions.Struct()
    struct.update(kwargs)

    return definitions.EnvironmentDefinition(
        kind=kind,
        configuration=struct,
    )


_NOT_SET = object()


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

    return_value = _NOT_SET
    for result in stub.Run(request):
        for _log in result.logs:
            log = from_grpc(_log)
            log_store[log.source].append(log)

        if result.is_complete:
            if return_value is _NOT_SET:
                return_value = result.result
            else:
                raise ValueError("Sent the result twice")

    if return_value is _NOT_SET:
        raise ValueError("Never sent the result")
    else:
        return cast(definitions.SerializedObject, return_value)


def prepare_request(function, *args, **kwargs):
    import dill

    import __main__

    dill.settings["recurse"] = True

    # Make it seem like it originated from __main__
    setattr(__main__, function.__name__, function)
    function.__module__ = "__main__"
    function.__qualname__ = f"__main__.{function.__name__}"

    basic_function = partial(function, *args, **kwargs)
    environment = define_environment("virtualenv", requirements=[])
    return definitions.BoundFunction(
        function=to_serialized_object(basic_function, method="dill"),
        environments=[environment],
    )


def run_function(stub, function, *args, log_handler=None, **kwargs):
    request = prepare_request(function, *args, **kwargs)

    user_logs: List[Log] = [] if log_handler is None else log_handler
    result = run_request(stub, request, user_logs=user_logs)

    raw_user_logs = [log.message for log in user_logs if log.message]
    return from_grpc(result), raw_user_logs


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
        requirements.append(f"{REPO_DIR}")

    env_definition = define_environment("virtualenv", requirements=requirements)
    request = definitions.BoundFunction(
        function=to_serialized_object(
            partial(
                eval,
                "__import__('pyjokes').__version__",
            ),
            method="dill",
        ),
        environments=[env_definition],
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
        environments=[env_definition],
    )

    build_logs: List[Log] = []
    with pytest.raises(grpc.RpcError) as exc:
        run_request(stub, request, build_logs=build_logs)

    assert "Failure during 'pip install': Command" in exc.value.details()

    raw_logs = [log.message for log in build_logs]
    assert any("ERROR: Invalid requirement: '$$$$'" in raw_log for raw_log in raw_logs)


def test_user_logs_immediate(stub: definitions.IsolateStub, monkeypatch: Any) -> None:
    inherit_from_local(monkeypatch)

    env_definition = define_environment("virtualenv", requirements=["pyjokes==0.6.0"])
    request = definitions.BoundFunction(
        function=to_serialized_object(
            partial(
                exec,
                textwrap.dedent(
                    """
                import sys, pyjokes
                print(pyjokes.__version__)
                print("error error!", file=sys.stderr)
                """
                ),
            ),
            method="dill",
        ),
        environments=[env_definition],
    )

    user_logs: List[Log] = []
    run_request(stub, request, user_logs=user_logs)

    assert len(user_logs) == 2

    by_stream = {log.level: log.message for log in user_logs}
    assert by_stream[LogLevel.STDOUT] == "0.6.0"
    assert by_stream[LogLevel.STDERR] == "error error!"


def test_unknown_environment(stub: definitions.IsolateStub, monkeypatch: Any) -> None:
    inherit_from_local(monkeypatch)

    env_definition = define_environment("unknown")
    request = definitions.BoundFunction(
        function=to_serialized_object(
            partial(
                eval,
                "__import__('pyjokes').__version__",
            ),
            method="dill",
        ),
        environments=[env_definition],
    )

    with pytest.raises(grpc.RpcError) as exc:
        run_request(stub, request)

    assert exc.match("Unknown environment kind")


def test_invalid_param(stub: definitions.IsolateStub, monkeypatch: Any) -> None:
    inherit_from_local(monkeypatch)

    env_definition = define_environment("virtualenv", packages=["pyjokes==1.0"])
    request = definitions.BoundFunction(
        function=to_serialized_object(
            partial(
                eval,
                "__import__('pyjokes').__version__",
            ),
            method="dill",
        ),
        environments=[env_definition],
    )

    with pytest.raises(grpc.RpcError) as exc:
        run_request(stub, request)

    assert exc.match("unexpected keyword argument 'packages'")


@pytest.mark.parametrize("inherit_local", [True, False])
def test_server_multiple_envs(
    stub: definitions.IsolateStub,
    monkeypatch: Any,
    inherit_local: bool,
) -> None:
    inherit_from_local(monkeypatch, inherit_local)
    xtra_requirements = ["python-dateutil==2.8.2"]
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
    xtra_env_definition = define_environment(
        "virtualenv", requirements=xtra_requirements
    )
    request = definitions.BoundFunction(
        function=to_serialized_object(
            partial(
                eval,
                (
                    "__import__('pyjokes').__version__ + "
                    "' ' + "
                    "__import__('dateutil').__version__"
                ),
            ),
            method="dill",
        ),
        environments=[env_definition, xtra_env_definition],
    )

    raw_result = run_request(stub, request)

    assert from_grpc(raw_result) == "0.6.0 2.8.2"


@pytest.mark.parametrize("python_version", ["3.8"])
def test_agent_requirements_custom_version(
    stub: definitions.IsolateStub,
    monkeypatch: Any,
    python_version: str,
) -> None:
    requirements = ["pyjokes==0.6.0"]
    agent_requirements = ["dill==0.3.5.1", f"{REPO_DIR}[grpc]"]
    monkeypatch.setattr("isolate.server.server.AGENT_REQUIREMENTS", agent_requirements)

    env_definition = define_environment(
        "virtualenv",
        requirements=requirements,
        python_version=python_version,
    )
    request = definitions.BoundFunction(
        function=to_serialized_object(
            partial(
                eval,
                (
                    "__import__('sysconfig').get_python_version(), "
                    "__import__('pyjokes').__version__"
                ),
            ),
            method="dill",
        ),
        environments=[env_definition],
    )

    raw_result = run_request(stub, request)
    assert from_grpc(raw_result) == ("3.8", "0.6.0")


def test_agent_show_logs_from_agent_requirements(
    stub: definitions.IsolateStub,
    monkeypatch: Any,
) -> None:
    requirements = ["pyjokes==0.6.0"]
    agent_requirements = ["$$$$", f"{REPO_DIR}[grpc]"]
    monkeypatch.setattr("isolate.server.server.AGENT_REQUIREMENTS", agent_requirements)

    env_definition = define_environment(
        "virtualenv",
        requirements=requirements,
    )
    request = definitions.BoundFunction(
        function=to_serialized_object(
            partial(
                eval,
                (
                    "__import__('sysconfig').get_python_version(), "
                    "__import__('pyjokes').__version__"
                ),
            ),
            method="dill",
        ),
        environments=[env_definition],
    )

    build_logs: List[Log] = []
    with pytest.raises(grpc.RpcError) as exc:
        run_request(stub, request, build_logs=build_logs)

    assert "Failure during 'pip install': Command" in exc.value.details()

    raw_logs = [log.message for log in build_logs]
    assert any("ERROR: Invalid requirement: '$$$$'" in raw_log for raw_log in raw_logs)


def test_bridge_connection_reuse(
    stub: definitions.IsolateStub, monkeypatch: Any
) -> None:
    inherit_from_local(monkeypatch)

    first_env = define_environment(
        "virtualenv",
        requirements=["pyjokes==0.6.0"],
    )
    request = definitions.BoundFunction(
        setup_func=to_serialized_object(
            lambda: __import__("os").getpid(), method="cloudpickle"
        ),
        function=to_serialized_object(
            lambda process_pid: process_pid, method="cloudpickle"
        ),
        environments=[first_env],
    )

    initial_process_pid = from_grpc(run_request(stub, request))
    secondary_process_pid = from_grpc(run_request(stub, request))

    # Both of the functions should run in the same agent process
    assert initial_process_pid == secondary_process_pid

    # But if we run a third function that has a different environment
    # then its process should be different
    second_env = define_environment(
        "virtualenv",
        requirements=["pyjokes==0.5.0"],
    )
    request_2 = copy.deepcopy(request)
    request_2.environments.remove(first_env)
    request_2.environments.append(second_env)

    third_process_pid = from_grpc(run_request(stub, request_2))
    assert third_process_pid != initial_process_pid

    # As long as the environments are same, they are cached
    fourth_process_pid = from_grpc(run_request(stub, request_2))
    assert fourth_process_pid == third_process_pid


@pytest.mark.flaky(reruns=3)
def test_bridge_connection_reuse_logs(
    stub: definitions.IsolateStub, monkeypatch: Any
) -> None:
    inherit_from_local(monkeypatch)

    first_env = define_environment(
        "virtualenv",
        requirements=["pyjokes==0.6.0"],
    )
    request = definitions.BoundFunction(
        setup_func=to_serialized_object(
            lambda: print("setup"),
            method="cloudpickle",
        ),
        function=to_serialized_object(
            lambda _: print("run"),
            method="cloudpickle",
        ),
        environments=[first_env],
    )

    logs: List[Log] = []
    run_request(stub, request, user_logs=logs)
    run_request(stub, request, user_logs=logs)
    run_request(stub, request, user_logs=logs)

    str_logs = [log.message for log in logs if log.message]
    assert str_logs == [
        "setup",
        "run",
        "run",
        "run",
    ]


def print_logs_no_delay(num_lines, should_flush):
    for i in range(num_lines):
        print(i, flush=should_flush)

    return num_lines


@pytest.mark.parametrize("num_lines", [0, 1, 10, 100, 1000])
@pytest.mark.parametrize("should_flush", [True, False])
@pytest.mark.flaky(reruns=3)
def test_receive_complete_logs(
    stub: definitions.IsolateStub,
    monkeypatch: Any,
    num_lines: int,
    should_flush: bool,
) -> None:
    inherit_from_local(monkeypatch)
    result, logs = run_function(stub, print_logs_no_delay, num_lines, should_flush)
    assert result == num_lines
    assert logs == [str(i) for i in range(num_lines)]


def take_buffer(buffer):
    return buffer


def test_grpc_option_configuration(tmp_path, monkeypatch):
    inherit_from_local(monkeypatch)
    with monkeypatch.context() as ctx:
        ctx.setenv("ISOLATE_GRPC_CALL_MAX_SEND_MESSAGE_LENGTH", "100")
        ctx.setenv("ISOLATE_GRPC_CALL_MAX_RECEIVE_MESSAGE_LENGTH", "100")

        with pytest.raises(grpc.RpcError, match="Sent message larger than max"):
            with make_server(tmp_path) as stubs:
                run_function(stubs.isolate_stub, take_buffer, b"0" * 200)

    with monkeypatch.context() as ctx:
        ctx.setenv("ISOLATE_GRPC_CALL_MAX_SEND_MESSAGE_LENGTH", "5000")
        ctx.setenv("ISOLATE_GRPC_CALL_MAX_RECEIVE_MESSAGE_LENGTH", "5000")

        with make_server(tmp_path) as stubs:
            result, _ = run_function(stubs.isolate_stub, take_buffer, b"0" * 200)
            assert result == b"0" * 200


def test_health_check(health_stub: health.HealthStub) -> None:
    resp: health.HealthCheckResponse = health_stub.Check(
        health.HealthCheckRequest(service="")
    )
    assert resp.status == health.HealthCheckResponse.SERVING


def check_machine():
    import os

    return os.getpid()


def kill_machine():
    import os

    os._exit(1)


def get_pid_as_exc():
    import os

    raise ValueError(os.getpid())


def test_bridge_caching_when_undeerlying_channel_fails(
    stub: definitions.IsolateStub, monkeypatch: Any
) -> None:
    import os
    import time

    inherit_from_local(monkeypatch)
    pid_1, _ = run_function(stub, check_machine)
    pid_2, _ = run_function(stub, check_machine)
    assert pid_1 == pid_2  # Same bridge

    # Now send some faulty code that breaks the
    # running agent and thus invalidatathing the
    # bridge
    with pytest.raises(grpc.RpcError):
        run_function(stub, kill_machine)

    # Now we should get a new bridge
    pid_3, _ = run_function(stub, check_machine)
    assert pid_1 != pid_3

    # Even if there is a normal exception, the bridge
    # should be reused (since we can capture it and it
    # does not affect it badly).
    with pytest.raises(ValueError) as exc_info:
        run_function(stub, get_pid_as_exc)

    [pid_4] = exc_info.value.args
    assert pid_3 == pid_4

    # Ensure that outside factors are also accounted for
    # and the bridge is not reused
    os.kill(pid_4, 9)

    # And channels are kept fresh for a while (according
    # to gRPC spec they might fall into idle when there is
    # no exchange between client and server for a while but
    # that doesn't seem to happen to us? If it did, we would
    # add keepalive pings to the channel but not sure if we need
    # it now).
    pid_5 = run_function(stub, check_machine)
    assert pid_4 != pid_5

    time.sleep(10)  # I've tried up to 90, and it seems to work fine?
    # using 10 as it is the default keepalive time
    # which would mean the channel would normally be
    # fallen into the idle status?

    pid_6 = run_function(stub, check_machine)
    assert pid_5 == pid_6


def test_server_minimum_viable_proto_version(stub: definitions.IsolateStub) -> None:
    # The agent process needs dill (and isolate) to actually
    # deserialize the given function, so they need to be installed
    # when we are not inheriting the local environment.

    # protobuf<3 (the 2.x series) seems to use Python 2 only?
    requirements = ["protobuf>3,<4"]
    requirements.append("dill==0.3.5.1")
    requirements.append(f"{REPO_DIR}")

    env_definition = define_environment("virtualenv", requirements=requirements)
    request = definitions.BoundFunction(
        function=to_serialized_object(
            partial(eval, "1+2"),
            method="dill",
        ),
        environments=[env_definition],
    )

    raw_result = run_request(stub, request)
    assert from_grpc(raw_result) == 3


def send_unserializable_object():
    import sys

    return sys._getframe()


def raise_unserializable_object():
    import sys

    raise Exception("relevant information", sys._getframe())


def test_server_proper_error_delegation(
    stub: definitions.IsolateStub, monkeypatch: Any
) -> None:
    inherit_from_local(monkeypatch)

    user_logs: List[Any] = []
    with pytest.raises(grpc.RpcError) as exc_info:
        run_function(stub, send_unserializable_object, log_handler=user_logs)

    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert (
        "Error while serializing the execution result "
        "(object of type <class 'frame'>)."
    ) in exc_info.value.details()
    assert not user_logs

    user_logs = []
    with pytest.raises(grpc.RpcError) as exc_info:
        run_function(stub, raise_unserializable_object, log_handler=user_logs)

    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert (
        "Error while serializing the execution result "
        "(object of type <class 'Exception'>)."
    ) in exc_info.value.details()
    assert "relevant information" in "\n".join(log.message for log in user_logs)


def myfunc(path):
    import time

    with open(path, "w") as fobj:
        for _ in range(10):
            time.sleep(0.1)
            fobj.write("still alive")

        fobj.write("completed")


def test_server_submit(
    stub: definitions.IsolateStub,
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    import time

    inherit_from_local(monkeypatch)

    file = tmp_path / "file"

    request = definitions.SubmitRequest(
        function=prepare_request(myfunc, str(file)),
    )
    stub.Submit(request)
    time.sleep(5)
    assert "completed" in file.read_text()
    assert not list(stub.List(definitions.ListRequest()).tasks)


def myserver():
    import time

    while True:
        print("running")
        time.sleep(1)


def test_server_submit_server(
    stub: definitions.IsolateStub,
    monkeypatch: Any,
) -> None:
    inherit_from_local(monkeypatch)

    request = definitions.SubmitRequest(function=prepare_request(myserver))
    task_id = stub.Submit(request).task_id

    tasks = [task.task_id for task in stub.List(definitions.ListRequest()).tasks]
    assert task_id in tasks

    stub.Cancel(definitions.CancelRequest(task_id=task_id))

    assert not list(stub.List(definitions.ListRequest()).tasks)
