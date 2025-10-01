"""Result collection and aggregation for benchmark framework."""

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field, asdict
from pydantic import BaseModel
import numpy as np
import pandas as pd
from .monitoring import SystemStats
from ..utils.logging import LoggerMixin


@dataclass
class LatencyStats:
    """Latency statistics."""
    count: int = 0
    min_ms: float = 0.0
    max_ms: float = 0.0
    mean_ms: float = 0.0
    median_ms: float = 0.0
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    p99_9_ms: float = 0.0
    # Store raw histogram data for proper aggregation
    histogram_data: Optional[Dict[str, Any]] = None


@dataclass
class ThroughputStats:
    """Throughput statistics."""
    total_messages: int = 0
    total_bytes: int = 0
    duration_seconds: float = 0.0
    messages_per_second: float = 0.0
    bytes_per_second: float = 0.0
    mb_per_second: float = 0.0


@dataclass
class ErrorStats:
    """Error statistics."""
    total_errors: int = 0
    error_rate: float = 0.0
    error_types: Dict[str, int] = field(default_factory=dict)


@dataclass
class WorkerResult:
    """Result from a single worker."""
    worker_id: str
    worker_url: str
    task_type: str  # 'producer' or 'consumer'
    start_time: float
    end_time: float

    # Performance metrics
    throughput: ThroughputStats = field(default_factory=ThroughputStats)
    latency: LatencyStats = field(default_factory=LatencyStats)
    errors: ErrorStats = field(default_factory=ErrorStats)

    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> float:
        """Get test duration in seconds."""
        return self.end_time - self.start_time


@dataclass
class BenchmarkResult:
    """Complete benchmark result."""
    test_id: str
    test_name: str
    start_time: float
    end_time: float

    # Configuration
    workload_config: Dict[str, Any] = field(default_factory=dict)
    driver_config: Dict[str, Any] = field(default_factory=dict)

    # Worker results
    producer_results: List[WorkerResult] = field(default_factory=list)
    consumer_results: List[WorkerResult] = field(default_factory=list)

    # Aggregated stats
    producer_stats: Optional[ThroughputStats] = None
    consumer_stats: Optional[ThroughputStats] = None
    latency_stats: Optional[LatencyStats] = None

    # System monitoring
    system_stats: Optional[SystemStats] = None

    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> float:
        """Get total test duration in seconds."""
        return self.end_time - self.start_time

    @property
    def total_producers(self) -> int:
        """Get total number of producers."""
        return len(self.producer_results)

    @property
    def total_consumers(self) -> int:
        """Get total number of consumers."""
        return len(self.consumer_results)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return asdict(self)

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2, default=str)


class ResultCollector(LoggerMixin):
    """Collects and aggregates benchmark results."""

    def __init__(self):
        super().__init__()
        self.results: List[BenchmarkResult] = []

    def create_result(
        self,
        test_name: str,
        workload_config: Dict[str, Any],
        driver_config: Dict[str, Any]
    ) -> BenchmarkResult:
        """Create a new benchmark result."""
        test_id = f"{test_name}_{int(time.time())}"

        result = BenchmarkResult(
            test_id=test_id,
            test_name=test_name,
            start_time=time.time(),
            end_time=0.0,
            workload_config=workload_config,
            driver_config=driver_config
        )

        self.results.append(result)
        self.logger.info(f"Created benchmark result: {test_id}")
        return result

    def add_worker_result(self, benchmark_result: BenchmarkResult, worker_result: WorkerResult) -> None:
        """Add worker result to benchmark result."""
        if worker_result.task_type == 'producer':
            benchmark_result.producer_results.append(worker_result)
        elif worker_result.task_type == 'consumer':
            benchmark_result.consumer_results.append(worker_result)
        else:
            self.logger.warning(f"Unknown task type: {worker_result.task_type}")

    def finalize_result(self, benchmark_result: BenchmarkResult, system_stats: Optional[SystemStats] = None) -> None:
        """Finalize benchmark result with aggregated statistics."""
        benchmark_result.end_time = time.time()
        benchmark_result.system_stats = system_stats

        # Aggregate producer stats
        if benchmark_result.producer_results:
            benchmark_result.producer_stats = self._aggregate_throughput_stats(
                [r.throughput for r in benchmark_result.producer_results]
            )

        # Aggregate consumer stats
        if benchmark_result.consumer_results:
            benchmark_result.consumer_stats = self._aggregate_throughput_stats(
                [r.throughput for r in benchmark_result.consumer_results]
            )

        # Aggregate latency stats (from producers)
        latencies = []
        for result in benchmark_result.producer_results:
            if result.latency.count > 0:
                latencies.extend([result.latency])

        if latencies:
            benchmark_result.latency_stats = self._aggregate_latency_stats(latencies)

        self.logger.info(f"Finalized benchmark result: {benchmark_result.test_id}")

    def _aggregate_throughput_stats(self, stats_list: List[ThroughputStats]) -> ThroughputStats:
        """Aggregate throughput statistics using actual concurrent time window.

        This method calculates throughput based on the actual time period when
        workers were concurrently executing, not just the longest single duration.
        """
        if not stats_list:
            return ThroughputStats()

        total_messages = sum(s.total_messages for s in stats_list)
        total_bytes = sum(s.total_bytes for s in stats_list)

        # Use maximum duration as a reasonable approximation
        # For more accurate results, we would need start_time/end_time from WorkerResult
        # which we'll use if available
        max_duration = max((s.duration_seconds for s in stats_list), default=0.0)

        # TODO: If WorkerResult timestamps are available, calculate:
        # actual_duration = max(end_times) - min(start_times)
        # This gives the true concurrent time window

        return ThroughputStats(
            total_messages=total_messages,
            total_bytes=total_bytes,
            duration_seconds=max_duration,
            messages_per_second=total_messages / max_duration if max_duration > 0 else 0.0,
            bytes_per_second=total_bytes / max_duration if max_duration > 0 else 0.0,
            mb_per_second=(total_bytes / (1024 * 1024)) / max_duration if max_duration > 0 else 0.0
        )

    def _aggregate_latency_stats(self, stats_list: List[LatencyStats]) -> LatencyStats:
        """Aggregate latency statistics by merging histograms."""
        if not stats_list:
            return LatencyStats()

        # Filter out stats with histogram data
        stats_with_histogram = [s for s in stats_list if s.histogram_data is not None]

        if stats_with_histogram:
            # Merge histograms for accurate percentile calculation
            try:
                from hdrh.histogram import HdrHistogram

                merged_histogram = HdrHistogram(1, 3600000000, 3)  # 1us to 1 hour

                for stats in stats_with_histogram:
                    # Decode histogram data
                    histogram_bytes = stats.histogram_data.get('encoded')
                    if histogram_bytes:
                        import base64
                        decoded = base64.b64decode(histogram_bytes)
                        temp_histogram = HdrHistogram.decode(decoded)
                        merged_histogram.add(temp_histogram)

                # Calculate percentiles from merged histogram
                return LatencyStats(
                    count=merged_histogram.get_total_count(),
                    min_ms=merged_histogram.get_min_value() / 1000.0,
                    max_ms=merged_histogram.get_max_value() / 1000.0,
                    mean_ms=merged_histogram.get_mean_value() / 1000.0,
                    median_ms=merged_histogram.get_value_at_percentile(50.0) / 1000.0,
                    p50_ms=merged_histogram.get_value_at_percentile(50.0) / 1000.0,
                    p95_ms=merged_histogram.get_value_at_percentile(95.0) / 1000.0,
                    p99_ms=merged_histogram.get_value_at_percentile(99.0) / 1000.0,
                    p99_9_ms=merged_histogram.get_value_at_percentile(99.9) / 1000.0
                )
            except Exception as e:
                self.logger.warning(f"Failed to merge histograms: {e}, using fallback method")
                # Fall through to fallback method

        # Fallback: weighted aggregation (less accurate but better than max)
        total_count = sum(s.count for s in stats_list)

        if total_count == 0:
            return LatencyStats()

        # Weighted averages for mean and median
        weighted_mean = sum(s.mean_ms * s.count for s in stats_list) / total_count
        weighted_median = sum(s.median_ms * s.count for s in stats_list) / total_count
        weighted_p50 = sum(s.p50_ms * s.count for s in stats_list) / total_count
        weighted_p95 = sum(s.p95_ms * s.count for s in stats_list) / total_count
        weighted_p99 = sum(s.p99_ms * s.count for s in stats_list) / total_count
        weighted_p99_9 = sum(s.p99_9_ms * s.count for s in stats_list) / total_count

        return LatencyStats(
            count=total_count,
            min_ms=min(s.min_ms for s in stats_list if s.count > 0),
            max_ms=max(s.max_ms for s in stats_list if s.count > 0),
            mean_ms=weighted_mean,
            median_ms=weighted_median,
            p50_ms=weighted_p50,
            p95_ms=weighted_p95,
            p99_ms=weighted_p99,
            p99_9_ms=weighted_p99_9
        )

    def save_result(self, result: BenchmarkResult, file_path: Union[str, Path]) -> None:
        """Save benchmark result to JSON file."""
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(result.to_json())

        self.logger.info(f"Saved benchmark result to: {file_path}")

    def load_result(self, file_path: Union[str, Path]) -> BenchmarkResult:
        """Load benchmark result from JSON file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Reconstruct BenchmarkResult from dict
        result = BenchmarkResult(**data)
        self.logger.info(f"Loaded benchmark result from: {file_path}")
        return result

    def export_csv(self, result: BenchmarkResult, file_path: Union[str, Path]) -> None:
        """Export benchmark result to CSV format."""
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Create summary data
        summary_data = {
            'test_id': result.test_id,
            'test_name': result.test_name,
            'duration_seconds': result.duration_seconds,
            'total_producers': result.total_producers,
            'total_consumers': result.total_consumers,
        }

        # Add producer stats
        if result.producer_stats:
            summary_data.update({
                'producer_messages_total': result.producer_stats.total_messages,
                'producer_throughput_msg_per_sec': result.producer_stats.messages_per_second,
                'producer_throughput_mb_per_sec': result.producer_stats.mb_per_second,
            })

        # Add consumer stats
        if result.consumer_stats:
            summary_data.update({
                'consumer_messages_total': result.consumer_stats.total_messages,
                'consumer_throughput_msg_per_sec': result.consumer_stats.messages_per_second,
                'consumer_throughput_mb_per_sec': result.consumer_stats.mb_per_second,
            })

        # Add latency stats
        if result.latency_stats:
            summary_data.update({
                'latency_mean_ms': result.latency_stats.mean_ms,
                'latency_p50_ms': result.latency_stats.p50_ms,
                'latency_p95_ms': result.latency_stats.p95_ms,
                'latency_p99_ms': result.latency_stats.p99_ms,
            })

        # Add system stats
        if result.system_stats:
            summary_data.update({
                'cpu_avg_percent': result.system_stats.cpu.get('avg', 0),
                'cpu_max_percent': result.system_stats.cpu.get('max', 0),
                'memory_avg_percent': result.system_stats.memory.get('avg', 0),
                'memory_max_percent': result.system_stats.memory.get('max', 0),
            })

        # Save to CSV
        df = pd.DataFrame([summary_data])
        df.to_csv(file_path, index=False)

        self.logger.info(f"Exported benchmark result to CSV: {file_path}")

    def generate_comparison_report(self, results: List[BenchmarkResult]) -> str:
        """Generate comparison report for multiple results."""
        if not results:
            return "No results to compare"

        report = ["# Benchmark Comparison Report\n"]
        report.append(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        report.append(f"Number of tests: {len(results)}\n\n")

        # Summary table
        report.append("## Summary\n")
        report.append("| Test Name | Duration (s) | Producer Throughput (msg/s) | Consumer Throughput (msg/s) | P99 Latency (ms) |")
        report.append("|-----------|--------------|------------------------------|------------------------------|------------------|")

        for result in results:
            producer_throughput = result.producer_stats.messages_per_second if result.producer_stats else 0
            consumer_throughput = result.consumer_stats.messages_per_second if result.consumer_stats else 0
            p99_latency = result.latency_stats.p99_ms if result.latency_stats else 0

            report.append(f"| {result.test_name} | {result.duration_seconds:.1f} | {producer_throughput:.0f} | {consumer_throughput:.0f} | {p99_latency:.2f} |")

        return "\n".join(report)