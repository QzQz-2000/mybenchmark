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

import threading
from hdrh.histogram import HdrHistogram
from .commands.counters_stats import CountersStats
from .commands.cumulative_latencies import CumulativeLatencies
from .commands.period_stats import PeriodStats


class LongAdder:
    """Thread-safe long adder (equivalent to Java's LongAdder)."""

    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()

    def increment(self):
        with self._lock:
            self._value += 1

    def add(self, value: int):
        with self._lock:
            self._value += value

    def sum(self) -> int:
        with self._lock:
            return self._value

    def sum_then_reset(self) -> int:
        with self._lock:
            value = self._value
            self._value = 0
            return value

    def reset(self):
        with self._lock:
            self._value = 0


class WorkerStats:
    """
    Worker statistics collector.
    """

    HIGHEST_TRACKABLE_VALUE = 60 * 1_000_000  # 60 seconds in microseconds

    def __init__(self, stats_logger=None):
        """
        Initialize worker statistics.

        :param stats_logger: Optional stats logger (Bookkeeper StatsLogger equivalent)
        """
        self.stats_logger = stats_logger

        # Recorders for histograms (using HdrHistogram)
        self.end_to_end_latency_recorder = HdrHistogram(1, 12 * 60 * 60 * 1_000_000, 5)  # 12 hours
        self.end_to_end_cumulative_latency_recorder = HdrHistogram(1, 12 * 60 * 60 * 1_000_000, 5)

        # Producer stats
        self.messages_sent = LongAdder()
        self.message_send_errors = LongAdder()
        self.bytes_sent = LongAdder()

        # Consumer stats
        self.messages_received = LongAdder()
        self.bytes_received = LongAdder()

        # Cumulative totals
        self.total_messages_sent = LongAdder()
        self.total_message_send_errors = LongAdder()
        self.total_messages_received = LongAdder()

        # Publish latency recorders
        self.publish_latency_recorder = HdrHistogram(1, self.HIGHEST_TRACKABLE_VALUE, 5)
        self.cumulative_publish_latency_recorder = HdrHistogram(1, self.HIGHEST_TRACKABLE_VALUE, 5)

        # Publish delay latency recorders
        self.publish_delay_latency_recorder = HdrHistogram(1, self.HIGHEST_TRACKABLE_VALUE, 5)
        self.cumulative_publish_delay_latency_recorder = HdrHistogram(1, self.HIGHEST_TRACKABLE_VALUE, 5)

    def get_stats_logger(self):
        """Get the stats logger."""
        return self.stats_logger

    def record_message_sent(self):
        """Record that a message was sent."""
        self.total_messages_sent.increment()

    def record_message_received(self, payload_length: int, end_to_end_latency_micros: int):
        """
        Record that a message was received.

        :param payload_length: Size of the payload in bytes
        :param end_to_end_latency_micros: End-to-end latency in microseconds
        """
        self.messages_received.increment()
        self.total_messages_received.increment()
        self.bytes_received.add(payload_length)

        if end_to_end_latency_micros > 0:
            self.end_to_end_cumulative_latency_recorder.record_value(end_to_end_latency_micros)
            self.end_to_end_latency_recorder.record_value(end_to_end_latency_micros)

    def to_period_stats(self) -> PeriodStats:
        """
        Get period statistics and reset period counters.

        :return: PeriodStats instance
        """
        stats = PeriodStats()

        stats.messages_sent = self.messages_sent.sum_then_reset()
        stats.message_send_errors = self.message_send_errors.sum_then_reset()
        stats.bytes_sent = self.bytes_sent.sum_then_reset()

        stats.messages_received = self.messages_received.sum_then_reset()
        stats.bytes_received = self.bytes_received.sum_then_reset()

        stats.total_messages_sent = self.total_messages_sent.sum()
        stats.total_message_send_errors = self.total_message_send_errors.sum()
        stats.total_messages_received = self.total_messages_received.sum()

        # Get interval histograms (resets the recorder)
        stats.publish_latency = self._get_interval_histogram(self.publish_latency_recorder)
        stats.publish_delay_latency = self._get_interval_histogram(self.publish_delay_latency_recorder)
        stats.end_to_end_latency = self._get_interval_histogram(self.end_to_end_latency_recorder)

        return stats

    def to_cumulative_latencies(self) -> CumulativeLatencies:
        """
        Get cumulative latency statistics.

        :return: CumulativeLatencies instance
        """
        latencies = CumulativeLatencies()
        latencies.publish_latency = self._get_interval_histogram(self.cumulative_publish_latency_recorder)
        latencies.publish_delay_latency = self._get_interval_histogram(self.cumulative_publish_delay_latency_recorder)
        latencies.end_to_end_latency = self._get_interval_histogram(self.end_to_end_cumulative_latency_recorder)
        return latencies

    def to_counters_stats(self) -> CountersStats:
        """
        Get counter statistics.

        :return: CountersStats instance
        """
        stats = CountersStats()
        stats.messages_sent = self.total_messages_sent.sum()
        stats.message_send_errors = self.total_message_send_errors.sum()
        stats.messages_received = self.total_messages_received.sum()
        return stats

    def reset_latencies(self):
        """Reset all latency recorders."""
        self.publish_latency_recorder = HdrHistogram(1, self.HIGHEST_TRACKABLE_VALUE, 5)
        self.cumulative_publish_latency_recorder = HdrHistogram(1, self.HIGHEST_TRACKABLE_VALUE, 5)
        self.publish_delay_latency_recorder = HdrHistogram(1, self.HIGHEST_TRACKABLE_VALUE, 5)
        self.cumulative_publish_delay_latency_recorder = HdrHistogram(1, self.HIGHEST_TRACKABLE_VALUE, 5)
        self.end_to_end_latency_recorder = HdrHistogram(1, 12 * 60 * 60 * 1_000_000, 5)
        self.end_to_end_cumulative_latency_recorder = HdrHistogram(1, 12 * 60 * 60 * 1_000_000, 5)

    def reset(self):
        """Reset all statistics."""
        self.reset_latencies()

        self.messages_sent.reset()
        self.message_send_errors.reset()
        self.bytes_sent.reset()
        self.messages_received.reset()
        self.bytes_received.reset()
        self.total_messages_sent.reset()
        self.total_messages_received.reset()

    def record_producer_failure(self):
        """Record a producer failure."""
        self.message_send_errors.increment()
        self.total_message_send_errors.increment()

    def record_producer_success(self, payload_length: int, intended_send_time_ns: int,
                                 send_time_ns: int, now_ns: int):
        """
        Record a successful producer send.

        :param payload_length: Size of the payload in bytes
        :param intended_send_time_ns: Intended send time in nanoseconds
        :param send_time_ns: Actual send time in nanoseconds
        :param now_ns: Current time in nanoseconds
        """
        self.messages_sent.increment()
        self.total_messages_sent.increment()
        self.bytes_sent.add(payload_length)

        # Calculate latency in microseconds
        latency_micros = min(self.HIGHEST_TRACKABLE_VALUE, (now_ns - send_time_ns) // 1000)
        self.publish_latency_recorder.record_value(latency_micros)
        self.cumulative_publish_latency_recorder.record_value(latency_micros)

        # Calculate send delay in microseconds
        send_delay_micros = min(self.HIGHEST_TRACKABLE_VALUE, (send_time_ns - intended_send_time_ns) // 1000)
        self.publish_delay_latency_recorder.record_value(send_delay_micros)
        self.cumulative_publish_delay_latency_recorder.record_value(send_delay_micros)

    @staticmethod
    def _get_interval_histogram(recorder: HdrHistogram) -> HdrHistogram:
        """
        Get interval histogram (simulates Java Recorder.getIntervalHistogram()).

        :param recorder: The recorder
        :return: A copy of the current histogram
        """
        # Use encode/decode for thread-safe copy
        # This creates a snapshot of the current histogram state
        encoded = recorder.encode()
        copy = HdrHistogram.decode(encoded)

        # Reset the original recorder for next interval
        recorder.reset()

        return copy
