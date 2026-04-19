from datetime import datetime


class BadStartLineError(Exception):
    def __init__(self, expected: int) -> None:
        self.expected = expected


class BadStepError(Exception):
    def __init__(self, last_step: int) -> None:
        self.last_step = last_step


class BadTimestampError(Exception):
    def __init__(self, last_timestamp: datetime) -> None:
        self.last_timestamp = last_timestamp


class MetricOwnedError(Exception):
    def __init__(self, key: str) -> None:
        self.key = key
