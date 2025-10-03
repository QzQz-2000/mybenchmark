# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import math
import threading
from typing import Callable, Optional


class UniformRateLimiter:
    """
    Provides a next operation time for rate limited operation streams.
    The rate limiter is thread safe and can be shared by all threads.
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
        # Atomically increment virtual_time
        with self._lock:
            curr_op_index = self._virtual_time
            self._virtual_time += 1

        # Initialize start time if needed (lazy initialization with double-checked locking)
        start = self._start
        if start is None:
            with self._start_lock:
                start = self._start
                if start is None:
                    start = self.nano_clock()
                    self._start = start

        return start + curr_op_index * self.interval_ns

    @staticmethod
    def uninterruptible_sleep_ns(intended_time: int):
        """
        Sleep until the intended time in nanoseconds.
        This is uninterruptible and will continue sleeping even if interrupted.
        """
        while True:
            sleep_ns = intended_time - time.perf_counter_ns()
            if sleep_ns <= 0:
                break
            # Convert nanoseconds to seconds for time.sleep
            time.sleep(sleep_ns / 1_000_000_000)
