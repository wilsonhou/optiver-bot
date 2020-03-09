import collections
import sys

from typing import Deque


class FrequencyLimiter(object):
    """Limit the frequency of events in a specified time interval."""

    def __init__(self, interval: float, limit: int):
        """Initialise a new instance of the FrequencyLimiter class."""
        self.events: Deque[float] = collections.deque()
        self.interval: float = interval
        self.limit: int = limit
        self.value: int = 0

    def check_event(self, now: float) -> bool:
        """Return True if the new event breaches the limit, False otherwise.

        This method should be called with a monotonically increasing sequence
        of times.
        """
        self.value += 1
        self.events.append(now)

        epsilon: float = sys.float_info.epsilon
        first: float = self.events[0]
        window_start: float = now - self.interval

        while (first - window_start) <= ((first if first > window_start else window_start) * epsilon):
            self.events.popleft()
            self.value -= 1
            first = self.events[0]

        return self.value > self.limit
