"""End-to-end tests for graceful shutdown behavior of IsolateServicer."""

import functools
import os
import signal
import subprocess
import sys
import threading
import time
from unittest.mock import Mock

import grpc
import pytest
from isolate.server.definitions.server_pb2 import BoundFunction, EnvironmentDefinition
from isolate.server.definitions.server_pb2_grpc import IsolateStub
from isolate.server.interface import to_serialized_object
from isolate.server.server import BridgeManager, IsolateServicer, RunnerAgent, RunTask


def create_run_request(func, teardown_func=None, *args, **kwargs):
    """Convert a Python function into a BoundFunction request for stub.Run()."""
    bound_function = functools.partial(func, *args, **kwargs)
    serialized_function = to_serialized_object(bound_function, method="cloudpickle")

    env_def = EnvironmentDefinition()
    env_def.kind = "local"

    request = BoundFunction()
    request.function.CopyFrom(serialized_function)
    request.environments.append(env_def)
    request.stream_logs = True
    if teardown_func is not None:
        request.teardown_func.CopyFrom(
            to_serialized_object(teardown_func, method="cloudpickle")
        )

    return request


@pytest.fixture
def servicer():
    """Create a real IsolateServicer instance for testing."""
    with BridgeManager() as bridge_manager:
        servicer = IsolateServicer(bridge_manager)
        yield servicer


@pytest.fixture
def isolate_server_subprocess(monkeypatch):
    """Set up a gRPC server with the IsolateServicer for testing."""
    # Find a free port
    import socket

    monkeypatch.setenv("ISOLATE_SHUTDOWN_GRACE_PERIOD", "2")

    with socket.socket() as s:
        s.bind(("", 0))
        port = s.getsockname()[1]

    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "isolate.server.server",
            "--single-use",
            "--port",
            str(port),
        ]
    )

    time.sleep(5)  # Wait for server to start
    yield process, port

    # Cleanup
    if process.poll() is None:
        process.terminate()
        process.wait(timeout=10)


def consume_responses(responses):
    def _consume():
        try:
            for response in responses:
                pass
        except grpc.RpcError:
            # Expected when connection is closed
            pass

    response_thread = threading.Thread(target=_consume, daemon=True)
    response_thread.start()


@pytest.mark.skip
def test_shutdown_with_terminate(servicer):
    task = RunTask(request=Mock(), future=Mock())
    servicer.background_tasks["TEST_BLOCKING"] = task
    task.agent = RunnerAgent(Mock(), Mock(), Mock(), Mock())
    task.agent.terminate = Mock(wraps=task.agent.terminate)
    servicer.shutdown()
    task.agent.terminate.assert_called_once()  # agent should be terminated


@pytest.mark.skip
def test_exit_on_client_close(isolate_server_subprocess):
    """Connect with grpc client, run a task and then close the client."""
    process, port = isolate_server_subprocess
    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = IsolateStub(channel)

    def fn():
        import time

        time.sleep(30)  # Simulate long-running task

    responses = stub.Run(create_run_request(fn))
    consume_responses(responses)

    # Give task time to start
    time.sleep(2)

    # there is a running grpc client connected to an isolate servicer which is
    # emitting responses from an agent running a infinite loop
    assert process.poll() is None, "Server should be running while client is connected"

    # Close the channel to simulate client disconnect
    channel.close()

    # Give time for the channel close to propagate and trigger termination
    time.sleep(1.0)

    try:
        # Wait for server process to exit
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        raise AssertionError("Server did not shut down after client disconnect")

    assert (
        process.poll() is not None
    ), "Server should have shut down after client disconnect"


def test_running_function_receives_sigterm(isolate_server_subprocess, tmp_path):
    """Test that the user provided code receives the SIGTERM"""
    process, port = isolate_server_subprocess
    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = IsolateStub(channel)

    # Send SIGTERM to the current process
    assert process.poll() is None, "Server should be running initially"

    sigterm_file_path = tmp_path.joinpath("sigterm_test")

    def teardown(path):
        import pathlib

        print(f"Teardown function called with path: {path}")

        pathlib.Path(path).touch()

    def func_with_teardown():
        import time

        time.sleep(30)  # Simulate long-running task

    teardown_func = functools.partial(teardown, str(sigterm_file_path))

    assert not sigterm_file_path.exists()

    responses = stub.Run(create_run_request(func_with_teardown, teardown_func))
    consume_responses(responses)
    time.sleep(2)  # Give task time to start

    os.kill(process.pid, signal.SIGTERM)
    process.wait(timeout=5)
    assert process.poll() is not None, "Server should have shut down after SIGTERM"
    assert (
        sigterm_file_path.exists()
    ), "Function should have received SIGTERM and created the file"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
