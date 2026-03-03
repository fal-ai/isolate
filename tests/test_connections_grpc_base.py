from pathlib import Path
from unittest.mock import Mock

from isolate.backends.local import LocalPythonEnvironment
from isolate.connections import LocalPythonGRPC
from isolate.logs import LogLevel, LogSource


def make_connection(tmp_path: Path) -> LocalPythonGRPC:
    environment = LocalPythonEnvironment()
    return LocalPythonGRPC(environment, tmp_path)


def test_abort_agent_logs_return_code_for_already_exited_process(
    tmp_path: Path,
) -> None:
    connection = make_connection(tmp_path)
    process = Mock()
    process.poll.return_value = 0
    process.returncode = 0
    connection._process = process

    connection.log = Mock()
    connection.abort_agent()

    process.terminate.assert_not_called()
    process.wait.assert_not_called()
    process.kill.assert_not_called()
    connection.log.assert_called_once_with(
        "Isolate agent finished (exit code: 0)",
        level=LogLevel.INFO,
        source=LogSource.BRIDGE,
    )
    assert connection._process is None


def test_abort_agent_logs_return_code_for_graceful_termination(tmp_path: Path) -> None:
    connection = make_connection(tmp_path)
    process = Mock()
    process.poll.return_value = None
    process.wait.return_value = -15
    connection._process = process

    connection.log = Mock()
    connection.abort_agent()

    process.terminate.assert_called_once()
    process.wait.assert_called_once()
    process.kill.assert_not_called()
    connection.log.assert_called_once_with(
        "Isolate agent finished (exit code: -15)",
        level=LogLevel.INFO,
        source=LogSource.BRIDGE,
    )
    assert connection._process is None


def test_abort_agent_logs_return_code_after_kill_fallback(tmp_path: Path) -> None:
    connection = make_connection(tmp_path)
    process = Mock()
    process.poll.return_value = None
    process.terminate.side_effect = RuntimeError("terminate failed")
    process.wait.return_value = -9
    connection._process = process

    connection.log = Mock()
    connection.abort_agent()

    process.terminate.assert_called_once()
    process.kill.assert_called_once()
    process.wait.assert_called_once()
    connection.log.assert_called_once_with(
        "Isolate agent finished (exit code: -9)",
        level=LogLevel.INFO,
        source=LogSource.BRIDGE,
    )
    assert connection._process is None
