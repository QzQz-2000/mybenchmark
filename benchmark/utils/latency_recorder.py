"""High-precision latency recording using HdrHistogram.

This module provides accurate latency measurement capabilities similar to
the original Java OMB implementation, using HdrHistogram for precise percentile calculations.
"""

import time
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
import numpy as np

try:
    from hdrh.histogram import HdrHistogram
    HDR_AVAILABLE = True
except ImportError:
    HDR_AVAILABLE = False
    HdrHistogram = None


@dataclass
class LatencySnapshot:
    """Snapshot of latency statistics."""
    count: int
    min_ms: float
    max_ms: float
    mean_ms: float
    stddev_ms: float
    p50_ms: float
    p75_ms: float
    p95_ms: float
    p99_ms: float
    p99_9_ms: float
    p99_99_ms: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'count': self.count,
            'min_ms': self.min_ms,
            'max_ms': self.max_ms,
            'mean_ms': self.mean_ms,
            'stddev_ms': self.stddev_ms,
            'p50_ms': self.p50_ms,
            'p75_ms': self.p75_ms,
            'p95_ms': self.p95_ms,
            'p99_ms': self.p99_ms,
            'p99_9_ms': self.p99_9_ms,
            'p99_99_ms': self.p99_99_ms,
        }


class LatencyRecorder:
    """High-precision latency recorder using HdrHistogram.

    This class provides accurate percentile calculations for latency measurements,
    similar to the original Java OMB implementation.
    """

    def __init__(
        self,
        lowest_trackable_value: int = 1,
        highest_trackable_value: int = 3600000,
        significant_figures: int = 3,
        use_hdr: bool = True
    ):
        """Initialize latency recorder.

        Args:
            lowest_trackable_value: Lowest latency value in microseconds (default 1µs)
            highest_trackable_value: Highest latency value in microseconds (default 1 hour)
            significant_figures: Number of significant figures for precision (1-5)
            use_hdr: Use HdrHistogram if available, fallback to numpy otherwise
        """
        self.use_hdr = use_hdr and HDR_AVAILABLE
        self._count = 0
        self._sum = 0.0
        self._sum_squares = 0.0
        self._min = float('inf')
        self._max = float('-inf')

        if self.use_hdr:
            # Use HdrHistogram for accurate percentiles
            self._histogram = HdrHistogram(
                lowest_trackable_value,
                highest_trackable_value,
                significant_figures
            )
        else:
            # Fallback to storing all values (memory intensive but accurate)
            self._values: List[float] = []

    def record_latency(self, latency_ms: float) -> None:
        """Record a latency measurement in milliseconds.

        Args:
            latency_ms: Latency value in milliseconds
        """
        if latency_ms < 0:
            return

        self._count += 1
        self._sum += latency_ms
        self._sum_squares += latency_ms * latency_ms
        self._min = min(self._min, latency_ms)
        self._max = max(self._max, latency_ms)

        if self.use_hdr:
            # Convert to microseconds for HdrHistogram
            latency_us = int(latency_ms * 1000)
            try:
                self._histogram.record_value(latency_us)
            except Exception:
                # Value out of range, just update basic stats
                pass
        else:
            self._values.append(latency_ms)

    def record_latency_micros(self, latency_us: int) -> None:
        """Record a latency measurement in microseconds.

        Args:
            latency_us: Latency value in microseconds
        """
        self.record_latency(latency_us / 1000.0)

    def get_snapshot(self) -> LatencySnapshot:
        """Get a snapshot of current latency statistics.

        Returns:
            LatencySnapshot with all percentiles calculated
        """
        if self._count == 0:
            return LatencySnapshot(
                count=0,
                min_ms=0.0,
                max_ms=0.0,
                mean_ms=0.0,
                stddev_ms=0.0,
                p50_ms=0.0,
                p75_ms=0.0,
                p95_ms=0.0,
                p99_ms=0.0,
                p99_9_ms=0.0,
                p99_99_ms=0.0
            )

        mean = self._sum / self._count
        variance = (self._sum_squares / self._count) - (mean * mean)
        stddev = np.sqrt(max(0, variance))

        if self.use_hdr:
            # Get percentiles from HdrHistogram (in microseconds)
            p50 = self._histogram.get_value_at_percentile(50.0) / 1000.0
            p75 = self._histogram.get_value_at_percentile(75.0) / 1000.0
            p95 = self._histogram.get_value_at_percentile(95.0) / 1000.0
            p99 = self._histogram.get_value_at_percentile(99.0) / 1000.0
            p99_9 = self._histogram.get_value_at_percentile(99.9) / 1000.0
            p99_99 = self._histogram.get_value_at_percentile(99.99) / 1000.0
        else:
            # Calculate percentiles from stored values
            sorted_values = np.sort(self._values)
            p50 = np.percentile(sorted_values, 50)
            p75 = np.percentile(sorted_values, 75)
            p95 = np.percentile(sorted_values, 95)
            p99 = np.percentile(sorted_values, 99)
            p99_9 = np.percentile(sorted_values, 99.9)
            p99_99 = np.percentile(sorted_values, 99.99)

        return LatencySnapshot(
            count=self._count,
            min_ms=self._min if self._min != float('inf') else 0.0,
            max_ms=self._max if self._max != float('-inf') else 0.0,
            mean_ms=mean,
            stddev_ms=stddev,
            p50_ms=p50,
            p75_ms=p75,
            p95_ms=p95,
            p99_ms=p99,
            p99_9_ms=p99_9,
            p99_99_ms=p99_99
        )

    def export_histogram(self) -> Optional[Dict[str, Any]]:
        """Export histogram data for aggregation.

        Returns:
            Dictionary with encoded histogram data, or None if not available
        """
        if not self.use_hdr or self._count == 0:
            return None

        try:
            import base64
            encoded = base64.b64encode(self._histogram.encode()).decode('utf-8')
            return {
                'encoded': encoded,
                'count': self._count
            }
        except Exception:
            return None

    def reset(self) -> None:
        """Reset all statistics."""
        self._count = 0
        self._sum = 0.0
        self._sum_squares = 0.0
        self._min = float('inf')
        self._max = float('-inf')

        if self.use_hdr:
            self._histogram.reset()
        else:
            self._values.clear()

    def merge(self, other: 'LatencyRecorder') -> None:
        """Merge another recorder into this one.

        Args:
            other: Another LatencyRecorder to merge
        """
        if other._count == 0:
            return

        self._count += other._count
        self._sum += other._sum
        self._sum_squares += other._sum_squares
        self._min = min(self._min, other._min)
        self._max = max(self._max, other._max)

        if self.use_hdr and other.use_hdr:
            # HdrHistogram supports efficient merging
            self._histogram.add(other._histogram)
        elif not self.use_hdr and not other.use_hdr:
            self._values.extend(other._values)


class EndToEndLatencyRecorder(LatencyRecorder):
    """Specialized recorder for end-to-end latency (producer to consumer).

    This tracks the latency from when a message is sent by the producer
    to when it's consumed by the consumer, similar to original OMB.
    """

    def __init__(self):
        """Initialize end-to-end latency recorder."""
        super().__init__(
            lowest_trackable_value=100,  # 0.1ms minimum
            highest_trackable_value=300_000_000,  # 5 minutes maximum
            significant_figures=3
        )

    def record_from_timestamp(self, send_timestamp_ms: float) -> None:
        """Record latency from a send timestamp embedded in message.

        Args:
            send_timestamp_ms: Timestamp when message was sent (milliseconds since epoch)
        """
        receive_time_ms = time.time() * 1000
        latency_ms = receive_time_ms - send_timestamp_ms

        if latency_ms > 0:  # Sanity check
            self.record_latency(latency_ms)


def create_latency_recorder(recorder_type: str = "default") -> LatencyRecorder:
    """Factory function to create appropriate latency recorder.

    Args:
        recorder_type: Type of recorder ("default", "end_to_end")

    Returns:
        LatencyRecorder instance
    """
    if recorder_type == "end_to_end":
        return EndToEndLatencyRecorder()
    else:
        return LatencyRecorder()


# Backwards compatibility with existing LatencyStats
def snapshot_to_legacy_stats(snapshot: LatencySnapshot, histogram_data: Optional[Dict[str, Any]] = None):
    """Convert LatencySnapshot to legacy LatencyStats format.

    Args:
        snapshot: LatencySnapshot to convert
        histogram_data: Optional histogram data for aggregation

    Returns:
        LatencyStats object compatible with existing code
    """
    from benchmark.core.results import LatencyStats

    return LatencyStats(
        count=snapshot.count,
        min_ms=snapshot.min_ms,
        max_ms=snapshot.max_ms,
        mean_ms=snapshot.mean_ms,
        median_ms=snapshot.p50_ms,
        p50_ms=snapshot.p50_ms,
        p95_ms=snapshot.p95_ms,
        p99_ms=snapshot.p99_ms,
        p99_9_ms=snapshot.p99_9_ms,
        histogram_data=histogram_data
    )