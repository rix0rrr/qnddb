import datetime
from abc import ABCMeta

class Cancel(metaclass=ABCMeta):
    """Contract for cancellation tokens."""
    @staticmethod
    def after_timeout(duration):
        return TimeoutCancellation(datetime.datetime.now() + duration)

    @staticmethod
    def never():
        return NeverCancellation()

    def is_cancelled(self):
        ...


class TimeoutCancellation(Cancel):
    """Cancellation token for a timeout."""

    def __init__(self, deadline):
        self.deadline = deadline

    def is_cancelled(self):
        return datetime.datetime.now() >= self.deadline


class NeverCancellation(Cancel):
    """Never cancellation."""

    def is_cancelled(self):
        return False


