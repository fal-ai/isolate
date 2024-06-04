import json


# NOTE: we probably should've created a proper `logging.getLogger` here,
# but it handling `source` would be not trivial, so we are better off
# just keeping it simple for now.
class IsolateLogger:
    def log(self, level, message, source):
        record = {
            "isolate_source": source.name,
            "level": level.name,
            "message": message,
        }
        print(json.dumps(record))


logger = IsolateLogger()
