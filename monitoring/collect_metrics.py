#!/usr/bin/env python3
"""
Kafka Performance Metrics Collector
Collects CPU, memory, and performance metrics from Prometheus during benchmark runs.
Based on research paper methodology: 1-second sampling, CSV export for analysis.
"""

import requests
import json
import time
import csv
from datetime import datetime
from typing import Dict, List, Optional
import sys

class MetricsCollector:
    def __init__(self, prometheus_url: str = "http://localhost:9090"):
        self.prometheus_url = prometheus_url
        self.metrics_queries = {
            # CPU metrics (from JMX)
            "cpu_usage_percent": 'rate(process_cpu_seconds_total{job="kafka-jmx"}[1m]) * 100',

            # JVM Memory metrics
            "jvm_heap_used_mb": 'jvm_memory_heap_used{job="kafka-jmx"} / 1024 / 1024',
            "jvm_heap_max_mb": 'jvm_memory_heap_max{job="kafka-jmx"} / 1024 / 1024',
            "jvm_heap_usage_percent": '(jvm_memory_heap_used{job="kafka-jmx"} / jvm_memory_heap_max{job="kafka-jmx"}) * 100',
            "jvm_nonheap_used_mb": 'jvm_memory_nonheap_used{job="kafka-jmx"} / 1024 / 1024',

            # Kafka throughput metrics
            "messages_in_per_sec": 'rate(kafka_broker_topic_messagesinpersec_total{job="kafka-jmx"}[1m])',
            "bytes_in_per_sec": 'rate(kafka_broker_topic_bytesinpersec_total{job="kafka-jmx"}[1m])',
            "bytes_out_per_sec": 'rate(kafka_broker_topic_bytesoutpersec_total{job="kafka-jmx"}[1m])',

            # Kafka latency metrics (p99)
            "produce_latency_p99_ms": 'kafka_network_request_totaltimems_percentile{request="Produce",percentile="99",job="kafka-jmx"}',
            "fetch_latency_p99_ms": 'kafka_network_request_totaltimems_percentile{request="FetchConsumer",percentile="99",job="kafka-jmx"}',

            # GC metrics (if available)
            "gc_count_per_sec": 'rate(jvm_gc_collection_count{job="kafka-jmx"}[1m])',
            "gc_time_percent": 'rate(jvm_gc_collection_seconds_sum{job="kafka-jmx"}[1m]) * 100',
        }

    def query_prometheus(self, query: str, timestamp: Optional[float] = None) -> List[Dict]:
        """Query Prometheus for a specific metric at a given timestamp."""
        try:
            params = {"query": query}
            if timestamp:
                params["time"] = timestamp

            resp = requests.get(
                f"{self.prometheus_url}/api/v1/query",
                params=params,
                timeout=5
            )
            data = resp.json()

            if data.get('status') != 'success':
                return []

            return data.get('data', {}).get('result', [])
        except Exception as e:
            print(f"Error querying Prometheus: {e}", file=sys.stderr)
            return []

    def query_range(self, query: str, start_time: float, end_time: float, step: str = "1s") -> List[Dict]:
        """Query Prometheus for a metric over a time range."""
        try:
            resp = requests.get(
                f"{self.prometheus_url}/api/v1/query_range",
                params={
                    "query": query,
                    "start": start_time,
                    "end": end_time,
                    "step": step
                },
                timeout=30
            )
            data = resp.json()

            if data.get('status') != 'success':
                return []

            return data.get('data', {}).get('result', [])
        except Exception as e:
            print(f"Error querying Prometheus range: {e}", file=sys.stderr)
            return []

    def collect_instant_metrics(self) -> Dict[str, float]:
        """Collect current values for all metrics."""
        metrics = {}

        for metric_name, query in self.metrics_queries.items():
            results = self.query_prometheus(query)

            if results:
                # Average across all brokers if multiple instances
                values = [float(r['value'][1]) for r in results]
                metrics[metric_name] = sum(values) / len(values) if values else 0.0
            else:
                metrics[metric_name] = 0.0

        return metrics

    def collect_range_metrics(self, start_time: float, end_time: float) -> Dict[str, List[tuple]]:
        """Collect time-series data for all metrics over a time range."""
        all_metrics = {}

        for metric_name, query in self.metrics_queries.items():
            results = self.query_range(query, start_time, end_time)

            if results:
                # Aggregate across all brokers by averaging at each timestamp
                time_series = {}
                for result in results:
                    for timestamp, value in result.get('values', []):
                        if timestamp not in time_series:
                            time_series[timestamp] = []
                        time_series[timestamp].append(float(value))

                # Average values at each timestamp
                all_metrics[metric_name] = [
                    (ts, sum(vals) / len(vals))
                    for ts, vals in sorted(time_series.items())
                ]
            else:
                all_metrics[metric_name] = []

        return all_metrics

    def export_to_csv(self, metrics_data: Dict[str, List[tuple]], output_file: str):
        """Export collected metrics to CSV format."""
        if not metrics_data:
            print(f"No metrics data to export", file=sys.stderr)
            return

        # Find all unique timestamps
        all_timestamps = set()
        for series in metrics_data.values():
            all_timestamps.update(ts for ts, _ in series)

        timestamps = sorted(all_timestamps)

        if not timestamps:
            print(f"No data points to export", file=sys.stderr)
            return

        # Create CSV
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)

            # Header
            header = ['timestamp', 'datetime'] + list(metrics_data.keys())
            writer.writerow(header)

            # Data rows
            for ts in timestamps:
                row = [ts, datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')]

                for metric_name in metrics_data.keys():
                    # Find value at this timestamp
                    series = metrics_data[metric_name]
                    value = next((v for t, v in series if t == ts), None)
                    row.append(value if value is not None else '')

                writer.writerow(row)

        print(f"✓ Exported metrics to {output_file}")
        print(f"  - Time range: {datetime.fromtimestamp(timestamps[0])} to {datetime.fromtimestamp(timestamps[-1])}")
        print(f"  - Data points: {len(timestamps)}")
        print(f"  - Metrics: {len(metrics_data)}")


def monitor_benchmark(test_name: str, duration_seconds: int, output_dir: str = "."):
    """
    Monitor a benchmark test for a specified duration and export metrics.

    Args:
        test_name: Name of the test (used in output filename)
        duration_seconds: How long to monitor (should cover benchmark duration)
        output_dir: Directory to save CSV output
    """
    collector = MetricsCollector()

    print(f"\n{'='*60}")
    print(f"Starting metrics collection for: {test_name}")
    print(f"Duration: {duration_seconds} seconds")
    print(f"{'='*60}\n")

    # Record start time
    start_time = time.time()
    print(f"Start time: {datetime.fromtimestamp(start_time)}")

    # Wait for benchmark to complete
    print(f"Monitoring for {duration_seconds} seconds...")
    time.sleep(duration_seconds)

    # Record end time
    end_time = time.time()
    print(f"End time: {datetime.fromtimestamp(end_time)}")

    # Collect metrics for the time range
    print("\nCollecting metrics from Prometheus...")
    metrics_data = collector.collect_range_metrics(start_time, end_time)

    # Export to CSV
    output_file = f"{output_dir}/{test_name}_{int(start_time)}.csv"
    collector.export_to_csv(metrics_data, output_file)

    # Calculate summary statistics
    print("\n" + "="*60)
    print("Summary Statistics:")
    print("="*60)
    for metric_name, series in metrics_data.items():
        if series:
            values = [v for _, v in series]
            avg = sum(values) / len(values)
            max_val = max(values)
            min_val = min(values)
            print(f"{metric_name:30} | Avg: {avg:8.2f} | Max: {max_val:8.2f} | Min: {min_val:8.2f}")
    print("="*60 + "\n")

    return output_file


if __name__ == "__main__":
    # Test the collector
    if len(sys.argv) > 1:
        test_name = sys.argv[1]
        duration = int(sys.argv[2]) if len(sys.argv) > 2 else 60
        output_dir = sys.argv[3] if len(sys.argv) > 3 else "."

        monitor_benchmark(test_name, duration, output_dir)
    else:
        # Demo: collect current metrics
        collector = MetricsCollector()
        print("\n=== Current Kafka Metrics ===\n")
        metrics = collector.collect_instant_metrics()
        for name, value in metrics.items():
            print(f"{name:30} : {value:.2f}")
