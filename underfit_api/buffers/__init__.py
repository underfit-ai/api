class BadStartLineError(Exception):
    def __init__(self, expected: int) -> None:
        self.expected = expected


class BadStepError(Exception):
    def __init__(self, last_step: int) -> None:
        self.last_step = last_step
