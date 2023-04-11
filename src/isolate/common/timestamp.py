from __future__ import annotations

from datetime import datetime, timezone

from google.protobuf.timestamp_pb2 import Timestamp


def from_datetime(time: datetime) -> Timestamp:
    timestamp = Timestamp()
    timestamp.FromDatetime(time)
    return timestamp


def to_datetime(timestamp: Timestamp) -> datetime:
    return timestamp.ToDatetime(tzinfo=timezone.utc)
