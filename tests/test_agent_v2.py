# mypy: ignore-errors

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from isolate.connections.grpc import agent_v2, definitions, interface

__TASK_AWAIT_DELAY = 0.05


@pytest.fixture(scope="module")
def grpc_add_to_server():
    return definitions.register_agent_v2


@pytest.fixture(scope="module")
def grpc_servicer():
    __max_timeout = 2.0

    with ThreadPoolExecutor(max_workers=1) as pool:
        servicer = agent_v2.AgentV2Servicer()
        done_event = threading.Event()
        task_processing_task = pool.submit(
            servicer.process_tasks,
            done_event,
        )
        try:
            yield servicer
        finally:
            done_event.set()
            task_processing_task.result(timeout=__max_timeout)


@pytest.fixture(scope="module")
def grpc_stub_cls(grpc_channel):
    return definitions.AgentV2Stub


def test_create_task(grpc_stub, grpc_servicer) -> None:
    request = definitions.CreateTaskRequest(
        function=interface.to_serialized_object(
            lambda: 42,
            method="dill",
        )
    )
    response = grpc_stub.CreateTask(request)
    task_id = response.task_id
    time.sleep(__TASK_AWAIT_DELAY)

    request = definitions.CheckTaskRequest(task_id=task_id)
    response = grpc_stub.CheckTask(request)
    assert response.status == definitions.TaskStatus.COMPLETED
    assert interface.from_grpc(response.result) == 42

    # Check that the response is still there.
    response = grpc_stub.CheckTask(request)
    assert response.status == definitions.TaskStatus.COMPLETED
    assert interface.from_grpc(response.result) == 42


def test_create_task_long_running(grpc_stub, grpc_servicer) -> None:
    long_run_delay = __TASK_AWAIT_DELAY * 10

    request = definitions.CreateTaskRequest(
        function=interface.to_serialized_object(
            lambda: time.sleep(long_run_delay),
            method="dill",
        )
    )
    response = grpc_stub.CreateTask(request)
    task_id = response.task_id

    time.sleep(__TASK_AWAIT_DELAY)

    request = definitions.CheckTaskRequest(task_id=task_id)
    response = grpc_stub.CheckTask(request)
    assert response.status == definitions.TaskStatus.RUNNING

    time.sleep(long_run_delay + __TASK_AWAIT_DELAY)

    request = definitions.CheckTaskRequest(task_id=task_id)
    response = grpc_stub.CheckTask(request)
    assert response.status == definitions.TaskStatus.COMPLETED
    assert interface.from_grpc(response.result) is None


def test_queue_multiple_tasks(grpc_stub, grpc_servicer) -> None:
    task_ids = []
    for n in range(5):
        request = definitions.CreateTaskRequest(
            function=interface.to_serialized_object(
                lambda: time.sleep(__TASK_AWAIT_DELAY * n),
                method="dill",
            )
        )
        response = grpc_stub.CreateTask(request)
        task_ids.append(response.task_id)

    time.sleep(__TASK_AWAIT_DELAY)

    # The first one might be completed already.
    request = definitions.CheckTaskRequest(task_id=task_ids[0])
    response = grpc_stub.CheckTask(request)
    assert response.status == definitions.TaskStatus.COMPLETED

    # But the last one should still be running.
    request = definitions.CheckTaskRequest(task_id=task_ids[-1])
    response = grpc_stub.CheckTask(request)
    assert response.status == definitions.TaskStatus.QUEUED

    # Check the queue status.
    request = definitions.AgentStatusRequest()
    response = grpc_stub.AgentStatus(request)
    assert len(task_ids) > response.num_queued > 0
    assert not response.idle_for

    # Wait for all to complete.
    for n, task_id in enumerate(task_ids):
        time.sleep(__TASK_AWAIT_DELAY * n)

    # Check the queue status.
    request = definitions.AgentStatusRequest()
    response = grpc_stub.AgentStatus(request)
    assert response.num_queued == 0
    assert response.idle_for > 0.0

    # Check each task to ensure they are completed.
    for task_id in task_ids:
        request = definitions.CheckTaskRequest(task_id=task_id)
        response = grpc_stub.CheckTask(request)
        assert response.status == definitions.TaskStatus.COMPLETED


def test_idle_for(grpc_stub, grpc_servicer) -> None:
    request = definitions.CreateTaskRequest(
        function=interface.to_serialized_object(
            lambda: 42,
            method="dill",
        )
    )
    grpc_stub.CreateTask(request)

    time.sleep(__TASK_AWAIT_DELAY)
    request = definitions.AgentStatusRequest()
    response = grpc_stub.AgentStatus(request)
    assert response.idle_for > 0.0

    time.sleep(__TASK_AWAIT_DELAY)
    request = definitions.AgentStatusRequest()
    response = grpc_stub.AgentStatus(request)
    assert response.idle_for > __TASK_AWAIT_DELAY

    time.sleep(__TASK_AWAIT_DELAY)
    request = definitions.AgentStatusRequest()
    response = grpc_stub.AgentStatus(request)
    assert response.idle_for > __TASK_AWAIT_DELAY * 2

    time.sleep(__TASK_AWAIT_DELAY)
    request = definitions.CreateTaskRequest(
        function=interface.to_serialized_object(
            lambda: time.sleep(__TASK_AWAIT_DELAY * 5),
            method="dill",
        )
    )
    grpc_stub.CreateTask(request)

    request = definitions.AgentStatusRequest()
    response = grpc_stub.AgentStatus(request)
    assert not response.idle_for

    time.sleep(__TASK_AWAIT_DELAY)
    request = definitions.AgentStatusRequest()
    response = grpc_stub.AgentStatus(request)
    assert not response.idle_for

    time.sleep(__TASK_AWAIT_DELAY * 5)
    request = definitions.AgentStatusRequest()
    response = grpc_stub.AgentStatus(request)
    assert response.idle_for > 0.0
