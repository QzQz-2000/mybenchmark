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
from .histogram_recorder import HistogramRecorder


class LongAdder:
    """Thread-safe long adder."""

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
    统一使用毫秒(ms)作为延迟单位
    """

    HIGHEST_TRACKABLE_VALUE = 60 * 1_000  # 60 seconds in milliseconds

    def __init__(self, stats_logger=None):
        """
        Initialize worker statistics.

        :param stats_logger: Optional stats logger (Bookkeeper StatsLogger equivalent)
        """
        self.stats_logger = stats_logger

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
        self.total_bytes_sent = LongAdder()
        self.total_messages_received = LongAdder()
        self.total_bytes_received = LongAdder()

        # 🚀 使用双缓冲Recorder（模仿Java版本）- 周期统计极快！
        # 所有延迟单位统一为毫秒(ms)
        self.publish_latency_recorder = HistogramRecorder(1, self.HIGHEST_TRACKABLE_VALUE, 5)
        self.publish_delay_latency_recorder = HistogramRecorder(1, self.HIGHEST_TRACKABLE_VALUE, 5)
        self.end_to_end_latency_recorder = HistogramRecorder(1, 12 * 60 * 60 * 1_000, 5)  # 12 hours in ms

        # Cumulative histograms（累积所有数据）
        self.cumulative_publish_latency = HdrHistogram(1, self.HIGHEST_TRACKABLE_VALUE, 5)
        self.cumulative_publish_delay_latency = HdrHistogram(1, self.HIGHEST_TRACKABLE_VALUE, 5)
        self.cumulative_end_to_end_latency = HdrHistogram(1, 12 * 60 * 60 * 1_000, 5)  # 12 hours in ms

        # Lock for histogram operations
        self.histogram_lock = threading.Lock()

    def get_stats_logger(self):
        """Get the stats logger."""
        return self.stats_logger

    def record_message_sent(self):
        """Record that a message was sent."""
        self.total_messages_sent.increment()

    def record_message_received(self, payload_length: int, end_to_end_latency_millis: int):
        """
        Record that a message was received.

        :param payload_length: Size of the payload in bytes
        :param end_to_end_latency_millis: End-to-end latency in milliseconds
        """
        self.messages_received.increment()
        self.total_messages_received.increment()
        self.bytes_received.add(payload_length)
        self.total_bytes_received.add(payload_length)

        if end_to_end_latency_millis > 0:
            # Recorder内部已有锁，不需要额外加锁
            self.end_to_end_latency_recorder.record_value(end_to_end_latency_millis)
            # 同时记录到cumulative
            with self.histogram_lock:
                self.cumulative_end_to_end_latency.record_value(end_to_end_latency_millis)

    def to_period_stats(self) -> PeriodStats:
        """
        Get period statistics and reset period counters.

        :return: PeriodStats instance
        """
        import time
        import logging
        logger = logging.getLogger(__name__)

        start = time.perf_counter()
        stats = PeriodStats()

        stats.messages_sent = self.messages_sent.sum_then_reset()
        stats.message_send_errors = self.message_send_errors.sum_then_reset()
        stats.bytes_sent = self.bytes_sent.sum_then_reset()

        stats.messages_received = self.messages_received.sum_then_reset()
        stats.bytes_received = self.bytes_received.sum_then_reset()

        stats.total_messages_sent = self.total_messages_sent.sum()
        stats.total_message_send_errors = self.total_message_send_errors.sum()
        stats.total_messages_received = self.total_messages_received.sum()

        t1 = time.perf_counter()
        # 🚀 使用Recorder的get_interval_histogram()，极快的O(1)指针交换！
        stats.publish_latency = self.publish_latency_recorder.get_interval_histogram()
        t2 = time.perf_counter()
        stats.publish_delay_latency = self.publish_delay_latency_recorder.get_interval_histogram()
        t3 = time.perf_counter()
        stats.end_to_end_latency = self.end_to_end_latency_recorder.get_interval_histogram()
        t4 = time.perf_counter()

        total_time = t4 - start
        logger.info(f"⏱️  to_period_stats() took {total_time*1000:.1f}ms (pub: {(t2-t1)*1000:.1f}ms, delay: {(t3-t2)*1000:.1f}ms, e2e: {(t4-t3)*1000:.1f}ms)")

        return stats

    def to_cumulative_latencies(self) -> CumulativeLatencies:
        """
        Get cumulative latency statistics.

        :return: CumulativeLatencies instance
        """
        latencies = CumulativeLatencies()
        # 直接返回累积直方图的拷贝（需要锁保护读取）
        with self.histogram_lock:
            latencies.publish_latency = self._copy_histogram(self.cumulative_publish_latency)
            latencies.publish_delay_latency = self._copy_histogram(self.cumulative_publish_delay_latency)
            latencies.end_to_end_latency = self._copy_histogram(self.cumulative_end_to_end_latency)
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
        # 重置Recorders（周期统计）
        self.publish_latency_recorder.reset()
        self.publish_delay_latency_recorder.reset()
        self.end_to_end_latency_recorder.reset()

        # 重置累积直方图
        with self.histogram_lock:
            self.cumulative_publish_latency = HdrHistogram(1, self.HIGHEST_TRACKABLE_VALUE, 5)
            self.cumulative_publish_delay_latency = HdrHistogram(1, self.HIGHEST_TRACKABLE_VALUE, 5)
            self.cumulative_end_to_end_latency = HdrHistogram(1, 12 * 60 * 60 * 1_000, 5)  # 12 hours in ms

    def reset(self):
        """Reset all statistics."""
        self.reset_latencies()

        self.messages_sent.reset()
        self.message_send_errors.reset()
        self.bytes_sent.reset()
        self.messages_received.reset()
        self.bytes_received.reset()
        self.total_messages_sent.reset()
        self.total_message_send_errors.reset()
        self.total_bytes_sent.reset()
        self.total_messages_received.reset()
        self.total_bytes_received.reset()

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
        self.total_bytes_sent.add(payload_length)

        # Calculate latency in milliseconds
        latency_millis = min(self.HIGHEST_TRACKABLE_VALUE, (now_ns - send_time_ns) // 1_000_000)
        send_delay_millis = min(self.HIGHEST_TRACKABLE_VALUE, (send_time_ns - intended_send_time_ns) // 1_000_000)

        # Recorder内部已有锁，不需要额外加锁
        self.publish_latency_recorder.record_value(latency_millis)
        self.publish_delay_latency_recorder.record_value(send_delay_millis)

        # 同时记录到cumulative histograms（需要锁保护）
        with self.histogram_lock:
            self.cumulative_publish_latency.record_value(latency_millis)
            self.cumulative_publish_delay_latency.record_value(send_delay_millis)

    def _copy_histogram(self, histogram: HdrHistogram) -> HdrHistogram:
        """
        Create a copy of a histogram (used for cumulative latencies).

        :param histogram: The histogram to copy
        :return: A copy of the histogram
        """
        # 使用encode/decode复制（调用者需要确保加锁）
        try:
            encoded = histogram.encode()
            return HdrHistogram.decode(encoded)
        except Exception as e:
            # If copy fails, return empty histogram
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to copy histogram: {e}, returning empty histogram")
            return HdrHistogram(1, 60 * 1_000, 5)  # 60 seconds in ms
