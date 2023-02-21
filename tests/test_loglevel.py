import pytest

from isolate.logs import LogLevel


def test_level_gt_comparison():
    assert LogLevel.INFO > LogLevel.DEBUG


def test_level_lt_comparison():
    assert LogLevel.WARNING < LogLevel.ERROR
