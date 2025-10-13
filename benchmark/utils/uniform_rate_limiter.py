import time
import math
import threading
from typing import Callable, Optional


class UniformRateLimiter:
    """
    Provides a next operation time for rate limited operation streams.
    Each process/thread creates its own instance for independent rate limiting.
    """

    ONE_SEC_IN_NS = 1_000_000_000  # 1 second in nanoseconds

    def __init__(self, ops_per_sec: float, nano_clock: Optional[Callable[[], int]] = None):
        if math.isnan(ops_per_sec) or math.isinf(ops_per_sec):
            raise ValueError("ops_per_sec cannot be NaN or Infinite")
        if ops_per_sec <= 0:
            raise ValueError("ops_per_sec must be greater than 0")

        self.ops_per_sec = ops_per_sec
        self.interval_ns = round(self.ONE_SEC_IN_NS / ops_per_sec)
        self.nano_clock = nano_clock if nano_clock is not None else time.perf_counter_ns

        self._start = None
        self._virtual_time = 0
        self._lock = threading.Lock()
        self._start_lock = threading.Lock()

    def get_ops_per_sec(self) -> float:
        return self.ops_per_sec

    def get_interval_ns(self) -> int:
        return self.interval_ns

    def acquire(self) -> int:
        """
        Acquire the next send time for rate-limited operation.
        Thread-safe implementation matching Java's AtomicLongFieldUpdater behavior.

        :return: Intended send time in nanoseconds
        """
        with self._lock:
            # Atomically increment virtual time and get current operation index
            curr_op_index = self._virtual_time
            self._virtual_time += 1

            # Initialize start time on first call (CAS equivalent)
            if self._start is None:
                self._start = self.nano_clock()

        # Calculate next send time: start + (operation_index * interval)
        # This matches Java: start + currOpIndex * intervalNs
        return self._start + curr_op_index * self.interval_ns

    @staticmethod
    def uninterruptible_sleep_ns(intended_time: int):
        """
        Sleep until the intended time in nanoseconds.
        """
        sleep_ns = intended_time - time.perf_counter_ns()
        if sleep_ns > 0:
            # Convert nanoseconds to seconds for time.sleep
            time.sleep(sleep_ns / 1_000_000_000)
