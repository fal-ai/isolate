import json

from isolate.logger import IsolateLogger


def test_logger(monkeypatch):
    labels = {
        "foo": "$MYENVVAR1",
        "bar": "$MYENVVAR2",
        "baz": "baz",
        "qux": "qux",
    }
    monkeypatch.setenv("MYENVVAR1", "myenvvar1")
    monkeypatch.setenv("MYENVVAR2", "myenvvar2")
    monkeypatch.setenv("ISOLATE_LOG_LABELS", json.dumps(labels))
    logger = IsolateLogger()
    assert logger.log_labels == {
        "foo": "myenvvar1",
        "bar": "myenvvar2",
        "baz": "baz",
        "qux": "qux",
    }
