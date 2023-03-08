import pytest

from isolate.connections.grpc import definitions
from isolate.logs import LogLevel


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
