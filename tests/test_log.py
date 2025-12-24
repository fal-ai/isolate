from datetime import datetime, timezone

from isolate.common import timestamp
from isolate.connections.grpc import definitions
from isolate.logs import Log, LogLevel, LogSource


def test_log_default_timestamp():
    log = Log(message="message", source=LogSource.USER, level=LogLevel.DEBUG)
    assert log.timestamp is not None
    assert log.timestamp <= datetime.now(timezone.utc)


def test_timestamp_conversion():
    now = datetime.now(timezone.utc)
    now_timestamp = timestamp.from_datetime(now)
    assert now_timestamp.ToMilliseconds() == int(now.timestamp() * 1000.0)


def test_level_gt_comparison():
    assert LogLevel.INFO > LogLevel.DEBUG


def test_level_lt_comparison():
    assert LogLevel.WARNING < LogLevel.ERROR


def test_level_str():
    assert str(LogLevel.INFO) == "info"


def test_log_definition_conversion():
    message = definitions.Log(message="message", source=0, level=3)
    level_definition = definitions.LogLevel.Name(message.level)
    assert LogLevel[level_definition.upper()] == LogLevel.WARNING


def test_json_logs():
    log = Log(
        message='{"line": "This is a log line", "user_id": 123, "task": "test"}',
        source=LogSource.USER,
        level=LogLevel.INFO,
        is_json=True,
    )
    assert log.message_str() == "This is a log line"
    meta = log.message_meta()
    assert meta["user_id"] == 123
    assert meta["task"] == "test"
    assert "line" not in meta

    # Ensure metatada didn't modify the original message
    assert log.message_str() == "This is a log line"

    non_json_log = Log(
        message="This is a plain log line",
        source=LogSource.USER,
        level=LogLevel.INFO,
        is_json=False,
    )
    assert non_json_log.message_str() == "This is a plain log line"
    meta = non_json_log.message_meta()
    assert meta == {}

    malformed_json_log = Log(
        # Missing closing brace
        message='{"line": "This is a line", "user_id": 123, "task": "test"',
        source=LogSource.USER,
        level=LogLevel.INFO,
        is_json=True,
    )
    assert (
        malformed_json_log.message_str()
        == '{"line": "This is a line", "user_id": 123, "task": "test"'
    )
    meta = malformed_json_log.message_meta()
    assert meta == {}
