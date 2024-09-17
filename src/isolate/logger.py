import json
import os
from typing import Dict

from isolate.logs import LogLevel, LogSource


# NOTE: we probably should've created a proper `logging.getLogger` here,
# but it handling `source` would be not trivial, so we are better off
# just keeping it simple for now.
class IsolateLogger:
    def __init__(self, log_labels: Dict[str, str]):
        self.log_labels = log_labels

    def log(self, level: LogLevel, message: str, source: LogSource) -> None:
        record = {
            "isolate_source": source.name,
            "level": level.name,
            "message": message,
            **self.log_labels,
        }
        print(json.dumps(record))

    @classmethod
    def with_env_expanded(cls, labels: Dict[str, str]) -> "IsolateLogger":
        for key, value in labels.items():
            if value.startswith("$"):
                expanded = os.getenv(value[1:])
            else:
                expanded = value
            if expanded is not None:
                labels[key] = expanded

        return cls(labels)

    @classmethod
    def from_env(cls) -> "IsolateLogger":
        _labels: Dict[str, str] = {}
        raw = os.getenv("ISOLATE_LOG_LABELS")
        if raw:
            try:
                _labels = json.loads(raw)
            except json.JSONDecodeError:
                print("Failed to parse ISOLATE_LOG_LABELS")

        return cls.with_env_expanded(labels=_labels)


ENV_LOGGER = IsolateLogger.from_env()
