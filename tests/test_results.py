"""Test result collection and aggregation."""

import pytest
import tempfile
import json
from pathlib import Path

from benchmark.core.results import (
    ThroughputStats,
    LatencyStats,
    ErrorStats,
    WorkerResult,
    BenchmarkResult,
    ResultCollector
)


class TestThroughputStats:
    """Test throughput statistics."""

    def test_throughput_stats_basic(self):
        """Test basic throughput statistics."""
        stats = ThroughputStats(
            total_messages=1000,
            total_bytes=1024000,
            duration_seconds=10.0,
            messages_per_second=100.0,
            bytes_per_second=102400.0,
            mb_per_second=0.1
        )

        assert stats.total_messages == 1000
        assert stats.total_bytes == 1024000
        assert stats.duration_seconds == 10.0
        assert stats.messages_per_second == 100.0
        assert stats.mb_per_second == 0.1


class TestLatencyStats:
    """Test latency statistics."""

    def test_latency_stats_basic(self):
        """Test basic latency statistics."""
        stats = LatencyStats(
            count=100,
            min_ms=1.0,
            max_ms=50.0,
            mean_ms=10.0,
            p95_ms=25.0,
            p99_ms=40.0
        )

        assert stats.count == 100
        assert stats.min_ms == 1.0
        assert stats.max_ms == 50.0
        assert stats.mean_ms == 10.0
        assert stats.p95_ms == 25.0
        assert stats.p99_ms == 40.0


class TestErrorStats:
    """Test error statistics."""

    def test_error_stats_basic(self):
        """Test basic error statistics."""
        stats = ErrorStats(
            total_errors=5,
            error_rate=0.05,
            error_types={"ConnectionError": 3, "TimeoutError": 2}
        )

        assert stats.total_errors == 5
        assert stats.error_rate == 0.05
        assert stats.error_types["ConnectionError"] == 3
        assert stats.error_types["TimeoutError"] == 2


class TestWorkerResult:
    """Test worker result."""

    def test_worker_result_basic(self):
        """Test basic worker result."""
        result = WorkerResult(
            worker_id="worker-1",
            worker_url="http://localhost:8080",
            task_type="producer",
            start_time=1000.0,
            end_time=1010.0
        )

        assert result.worker_id == "worker-1"
        assert result.task_type == "producer"
        assert result.duration_seconds == 10.0

    def test_worker_result_with_stats(self):
        """Test worker result with statistics."""
        throughput = ThroughputStats(
            total_messages=500,
            total_bytes=512000,
            duration_seconds=5.0,
            messages_per_second=100.0,
            bytes_per_second=102400.0,
            mb_per_second=0.1
        )

        latency = LatencyStats(
            count=500,
            mean_ms=5.0,
            p99_ms=15.0
        )

        errors = ErrorStats(total_errors=2)

        result = WorkerResult(
            worker_id="worker-1",
            worker_url="http://localhost:8080",
            task_type="producer",
            start_time=1000.0,
            end_time=1005.0,
            throughput=throughput,
            latency=latency,
            errors=errors
        )

        assert result.throughput.total_messages == 500
        assert result.latency.mean_ms == 5.0
        assert result.errors.total_errors == 2


class TestBenchmarkResult:
    """Test benchmark result."""

    def test_benchmark_result_basic(self):
        """Test basic benchmark result."""
        result = BenchmarkResult(
            test_id="test-123",
            test_name="Test Benchmark",
            start_time=1000.0,
            end_time=1060.0
        )

        assert result.test_id == "test-123"
        assert result.test_name == "Test Benchmark"
        assert result.duration_seconds == 60.0
        assert result.total_producers == 0
        assert result.total_consumers == 0

    def test_benchmark_result_with_workers(self):
        """Test benchmark result with worker results."""
        result = BenchmarkResult(
            test_id="test-123",
            test_name="Test Benchmark",
            start_time=1000.0,
            end_time=1060.0
        )

        # Add producer results
        producer_result = WorkerResult(
            worker_id="producer-1",
            worker_url="http://localhost:8080",
            task_type="producer",
            start_time=1000.0,
            end_time=1030.0
        )
        result.producer_results.append(producer_result)

        # Add consumer results
        consumer_result = WorkerResult(
            worker_id="consumer-1",
            worker_url="http://localhost:8081",
            task_type="consumer",
            start_time=1000.0,
            end_time=1060.0
        )
        result.consumer_results.append(consumer_result)

        assert result.total_producers == 1
        assert result.total_consumers == 1

    def test_benchmark_result_serialization(self):
        """Test benchmark result JSON serialization."""
        result = BenchmarkResult(
            test_id="test-123",
            test_name="Test Benchmark",
            start_time=1000.0,
            end_time=1060.0,
            workload_config={"name": "test"},
            driver_config={"name": "kafka"}
        )

        # Test to_dict
        result_dict = result.to_dict()
        assert result_dict["test_id"] == "test-123"
        assert result_dict["workload_config"]["name"] == "test"

        # Test to_json
        result_json = result.to_json()
        assert isinstance(result_json, str)

        # Parse back
        parsed = json.loads(result_json)
        assert parsed["test_id"] == "test-123"


class TestResultCollector:
    """Test result collector."""

    def test_create_result(self):
        """Test creating a benchmark result."""
        collector = ResultCollector()

        result = collector.create_result(
            test_name="Test",
            workload_config={"name": "test"},
            driver_config={"name": "kafka"}
        )

        assert result.test_name == "Test"
        assert result.workload_config["name"] == "test"
        assert result.driver_config["name"] == "kafka"
        assert len(collector.results) == 1

    def test_add_worker_result(self):
        """Test adding worker results."""
        collector = ResultCollector()

        benchmark_result = collector.create_result(
            test_name="Test",
            workload_config={},
            driver_config={}
        )

        # Add producer result
        producer_result = WorkerResult(
            worker_id="producer-1",
            worker_url="http://localhost:8080",
            task_type="producer",
            start_time=1000.0,
            end_time=1030.0
        )
        collector.add_worker_result(benchmark_result, producer_result)

        # Add consumer result
        consumer_result = WorkerResult(
            worker_id="consumer-1",
            worker_url="http://localhost:8081",
            task_type="consumer",
            start_time=1000.0,
            end_time=1060.0
        )
        collector.add_worker_result(benchmark_result, consumer_result)

        assert len(benchmark_result.producer_results) == 1
        assert len(benchmark_result.consumer_results) == 1

    def test_finalize_result(self):
        """Test finalizing benchmark result."""
        collector = ResultCollector()

        benchmark_result = collector.create_result(
            test_name="Test",
            workload_config={},
            driver_config={}
        )

        # Add some worker results with stats
        throughput = ThroughputStats(
            total_messages=1000,
            total_bytes=1024000,
            duration_seconds=10.0,
            messages_per_second=100.0,
            bytes_per_second=102400.0,
            mb_per_second=0.1
        )

        latency = LatencyStats(
            count=1000,
            mean_ms=10.0,
            p99_ms=50.0
        )

        producer_result = WorkerResult(
            worker_id="producer-1",
            worker_url="http://localhost:8080",
            task_type="producer",
            start_time=1000.0,
            end_time=1010.0,
            throughput=throughput,
            latency=latency
        )

        consumer_result = WorkerResult(
            worker_id="consumer-1",
            worker_url="http://localhost:8081",
            task_type="consumer",
            start_time=1000.0,
            end_time=1020.0,
            throughput=throughput
        )

        collector.add_worker_result(benchmark_result, producer_result)
        collector.add_worker_result(benchmark_result, consumer_result)

        # Finalize
        collector.finalize_result(benchmark_result)

        # Check aggregated stats
        assert benchmark_result.producer_stats is not None
        assert benchmark_result.consumer_stats is not None
        assert benchmark_result.latency_stats is not None
        assert benchmark_result.end_time > benchmark_result.start_time

    def test_save_and_load_result(self):
        """Test saving and loading benchmark result."""
        collector = ResultCollector()

        result = collector.create_result(
            test_name="Test Save",
            workload_config={"name": "test"},
            driver_config={"name": "kafka"}
        )

        collector.finalize_result(result)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name

        try:
            # Save result
            collector.save_result(result, temp_path)

            # Load result
            loaded_result = collector.load_result(temp_path)

            assert loaded_result.test_name == result.test_name
            assert loaded_result.test_id == result.test_id

        finally:
            Path(temp_path).unlink()

    def test_export_csv(self):
        """Test exporting result to CSV."""
        collector = ResultCollector()

        result = collector.create_result(
            test_name="Test CSV",
            workload_config={},
            driver_config={}
        )

        # Add some stats
        result.producer_stats = ThroughputStats(
            total_messages=1000,
            messages_per_second=100.0,
            mb_per_second=0.1
        )

        result.latency_stats = LatencyStats(
            mean_ms=10.0,
            p99_ms=50.0
        )

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name

        try:
            collector.export_csv(result, temp_path)

            # Verify file exists and has content
            csv_content = Path(temp_path).read_text()
            assert "test_id" in csv_content
            assert "Test CSV" in csv_content

        finally:
            Path(temp_path).unlink()