import json

import pytest
from isolate.logger import IsolateLogger


@pytest.fixture
def log_labels():
    return {
        "foo": "$MYENVVAR1",
        "bar": "$MYENVVAR2",
        "baz": "baz",
        "qux": "$NOTTHERE",
    }


def test_logger_with_env_expanded(log_labels, monkeypatch):
    monkeypatch.setenv("MYENVVAR1", "myenvvar1")
    monkeypatch.setenv("MYENVVAR2", "myenvvar2")
    logger = IsolateLogger.with_env_expanded(log_labels)
    assert logger.log_labels == {
        "foo": "myenvvar1",
        "bar": "myenvvar2",
        "baz": "baz",
        "qux": "$NOTTHERE",
    }


def test_logger_from_env(log_labels, monkeypatch):
    monkeypatch.setenv("MYENVVAR1", "myenvvar1")
    monkeypatch.setenv("MYENVVAR2", "myenvvar2")
    monkeypatch.setenv("ISOLATE_LOG_LABELS", json.dumps(log_labels))
    logger = IsolateLogger.from_env()
    assert logger.log_labels == {
        "foo": "myenvvar1",
        "bar": "myenvvar2",
        "baz": "baz",
        "qux": "$NOTTHERE",
    }


def test_logger_direct(log_labels):
    logger = IsolateLogger(log_labels=log_labels)
    # should not do env expansion
    assert logger.log_labels == log_labels
