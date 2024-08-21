import json
import os


# NOTE: we probably should've created a proper `logging.getLogger` here,
# but it handling `source` would be not trivial, so we are better off
# just keeping it simple for now.
class IsolateLogger:
    def __init__(self):
        self.log_labels = {}
        raw = os.getenv("ISOLATE_LOG_LABELS")
        if raw:
            labels = json.loads(raw)
            for key, value in labels.items():
                if value.startswith("$"):
                    expanded = os.getenv(value[1:])
                else:
                    expanded = value
                self.log_labels[key] = expanded

    def log(self, level, message, source):
        record = {
            "isolate_source": source.name,
            "level": level.name,
            "message": message,
            **self.log_labels,
        }
        print(json.dumps(record))


logger = IsolateLogger()
