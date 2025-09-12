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


def create_run_request(func, stream_logs=True):
    """Convert a Python function into a BoundFunction request for stub.Run()."""
    bound_function = functools.partial(func)
    serialized_function = to_serialized_object(bound_function, method="cloudpickle")

    env_def = EnvironmentDefinition()
    env_def.kind = "local"

    request = BoundFunction()
    request.function.CopyFrom(serialized_function)
    request.environments.append(env_def)
    request.stream_logs = stream_logs

    return request


@pytest.fixture
def servicer():
    """Create a real IsolateServicer instance for testing."""
    with BridgeManager() as bridge_manager:
        servicer = IsolateServicer(bridge_manager)
        yield servicer


@pytest.fixture
def isolate_server_subprocess():
    """Set up a gRPC server with the IsolateServicer for testing."""

    os.environ["ISOLATE_SHUTDOWN_GRACE_PERIOD"] = "2"

    # Find a free port
    import socket

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

    time.sleep(0.1)  # Wait for server to start
    yield process, port

    # Cleanup
    if process.poll() is None:
        process.terminate()
        process.wait(timeout=5)


def test_shutdown_with_terminate(servicer):
    """Test shutdown confirms that terminate is called
    on servicer background tasks by initiate_shutdown"""
    task = RunTask(request=Mock())
    servicer.background_tasks["TEST_BLOCKING"] = task
    task.agent = RunnerAgent(Mock(), Mock(), Mock(), Mock())
    task.agent.terminate = Mock(wraps=task.agent.terminate)
    servicer.initiate_shutdown()  # default grace period
    # force_terminate is tested within runner_agent testing
    task.agent.terminate.assert_called_once()  # agent should be terminated


def test_exit_on_client_close(isolate_server_subprocess):
    """Connect with grpc client, run a task and then close the client."""
    process, port = isolate_server_subprocess
    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = IsolateStub(channel)

    def fn():
        while True:
            pass

    responses = stub.Run(create_run_request(fn))

    def consume_responses():
        try:
            for response in responses:
                print("Received response:", response)
        except grpc.RpcError:
            # Expected when connection is closed
            pass

    response_thread = threading.Thread(target=consume_responses, daemon=True)
    response_thread.start()

    # Give task time to start
    time.sleep(0.5)

    # there is a running grpc client connected to an isolate servicer which is
    # emitting responses from an agent running a infinite loop
    assert process.poll() is None, "Server should be running while client is connected"

    # Close the channel to simulate client disconnect
    channel.close()

    # Give time for the channel close to propagate and trigger termination
    time.sleep(1.0)

    try:
        # Wait for server process to exit
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        raise AssertionError("Server did not shut down after client disconnect")

    assert (
        process.poll() is not None
    ), "Server should have shut down after client disconnect"


def test_sigterm_termination(isolate_server_subprocess):
    """Test that the server shuts down gracefully on SIGTERM."""
    process, port = isolate_server_subprocess
    # Send SIGTERM to the current process
    assert process.poll() is None, "Server should be running initially"
    os.kill(process.pid, signal.SIGTERM)
    process.wait(timeout=5)
    assert process.poll() is not None, "Server should have shut down after SIGTERM"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
