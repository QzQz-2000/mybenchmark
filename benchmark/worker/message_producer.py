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
import logging
from typing import Optional, Callable

logger = logging.getLogger(__name__)


class MessageProducer:
    """
    Message producer with rate limiting.
    """

    def __init__(self, rate_limiter, stats, nano_clock: Optional[Callable[[], int]] = None):
        """
        Initialize message producer.

        :param rate_limiter: UniformRateLimiter instance
        :param stats: WorkerStats instance
        :param nano_clock: Optional nanosecond clock function
        """
        self.rate_limiter = rate_limiter
        self.stats = stats
        self.nano_clock = nano_clock if nano_clock is not None else time.perf_counter_ns

    def send_message(self, producer, key: Optional[str], payload: bytes):
        """
        Send a message with rate limiting.

        :param producer: BenchmarkProducer instance
        :param key: Optional message key
        :param payload: Message payload bytes
        """
        intended_send_time = self.rate_limiter.acquire()
        self._uninterruptible_sleep_ns(intended_send_time)
        send_time = self.nano_clock()

        # Send message asynchronously
        future = producer.send_async(key, payload)

        # Handle success/failure
        def on_success(_):
            self._success(len(payload), intended_send_time, send_time)

        def on_failure(exception):
            self._failure(exception)

        # Attach callbacks
        future.add_done_callback(lambda f: on_success(f.result()) if not f.exception() else on_failure(f.exception()))

    def _success(self, payload_length: int, intended_send_time: int, send_time: int):
        """Handle successful send."""
        now_ns = self.nano_clock()
        self.stats.record_producer_success(payload_length, intended_send_time, send_time, now_ns)

    def _failure(self, throwable: Exception):
        """Handle send failure."""
        self.stats.record_producer_failure()
        logger.warning(f"Write error on message: {throwable}")

    @staticmethod
    def _uninterruptible_sleep_ns(intended_time: int):
        """Sleep until intended time in nanoseconds."""
        while True:
            sleep_ns = intended_time - time.perf_counter_ns()
            if sleep_ns <= 0:
                break
            time.sleep(sleep_ns / 1_000_000_000)
