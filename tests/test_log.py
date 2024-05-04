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
