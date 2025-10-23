#!/usr/bin/env python3
"""
Automated Kafka Benchmark with Integrated Monitoring
Runs OpenMessaging benchmarks and collects performance metrics.
Based on research paper methodology: multiple runs, JVM cooldown, CSV export.
"""

import subprocess
import time
import os
import sys
from datetime import datetime
from pathlib import Path

# Add monitoring directory to path
sys.path.insert(0, str(Path(__file__).parent / "monitoring"))
from collect_metrics import MetricsCollector

class BenchmarkRunner:
    def __init__(self,
                 workload_dir: str = "./driver-kafka/kafka-test.yaml",
                 output_dir: str = "./benchmark_results",
                 prometheus_url: str = "http://localhost:9090"):
        self.workload_dir = workload_dir
        self.output_dir = output_dir
        self.collector = MetricsCollector(prometheus_url)

        # Create output directory
        Path(output_dir).mkdir(exist_ok=True)

    def wait_for_kafka_ready(self):
        """Wait for Kafka to be fully operational."""
        print("Checking Kafka status...")
        max_attempts = 30
        for i in range(max_attempts):
            try:
                metrics = self.collector.collect_instant_metrics()
                if metrics.get('messages_in_per_sec', 0) >= 0:  # Metric exists
                    print("✓ Kafka is ready")
                    return True
            except:
                pass

            if i < max_attempts - 1:
                print(f"  Waiting for Kafka... ({i+1}/{max_attempts})")
                time.sleep(2)

        print("⚠ Warning: Could not verify Kafka status, proceeding anyway")
        return True

    def jvm_cooldown(self, duration_seconds: int = 600):
        """Wait for JVM to cool down between tests (as per paper methodology)."""
        print(f"\n{'='*60}")
        print(f"JVM Cooldown Period: {duration_seconds} seconds ({duration_seconds/60:.0f} minutes)")
        print(f"This allows the JVM to stabilize and GC to complete.")
        print(f"{'='*60}\n")

        start = time.time()
        while time.time() - start < duration_seconds:
            remaining = duration_seconds - (time.time() - start)
            minutes = int(remaining // 60)
            seconds = int(remaining % 60)
            print(f"\r  Remaining: {minutes:2d}:{seconds:02d}", end='', flush=True)
            time.sleep(1)
        print("\n✓ Cooldown complete\n")

    def run_single_benchmark(self,
                            test_name: str,
                            producers: int,
                            consumers: int,
                            message_size: int = 1024,
                            duration_minutes: int = 5) -> dict:
        """Run a single benchmark test with monitoring."""

        print(f"\n{'='*70}")
        print(f"Running Test: {test_name}")
        print(f"  Producers: {producers}")
        print(f"  Consumers: {consumers}")
        print(f"  Message Size: {message_size} bytes")
        print(f"  Duration: {duration_minutes} minutes")
        print(f"{'='*70}\n")

        # Prepare benchmark command
        # Note: Adjust this command based on your actual benchmark tool
        benchmark_cmd = [
            "bin/benchmark",
            "--drivers", self.workload_dir,
            "--producers", str(producers),
            "--consumers", str(consumers),
            "--message-size", str(message_size),
            "--test-duration", str(duration_minutes)
        ]

        # Start time
        start_time = time.time()
        start_dt = datetime.fromtimestamp(start_time)

        print(f"Start time: {start_dt}")
        print(f"Running benchmark command: {' '.join(benchmark_cmd)}\n")

        # Start benchmark process (non-blocking)
        try:
            # Run benchmark in background
            process = subprocess.Popen(
                benchmark_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            # Monitor metrics while benchmark runs
            # Add buffer time for benchmark startup/shutdown
            monitor_duration = duration_minutes * 60 + 30

            print("Collecting metrics...")
            time.sleep(monitor_duration)

            # Wait for benchmark to complete
            stdout, stderr = process.communicate(timeout=30)

            # Record end time
            end_time = time.time()
            end_dt = datetime.fromtimestamp(end_time)

            print(f"\nEnd time: {end_dt}")
            print(f"Actual duration: {(end_time - start_time)/60:.1f} minutes")

            # Collect metrics for the exact time range
            print("\nCollecting metrics from Prometheus...")
            metrics_data = self.collector.collect_range_metrics(start_time, end_time)

            # Export to CSV
            timestamp_str = int(start_time)
            metrics_file = f"{self.output_dir}/{test_name}_{timestamp_str}_metrics.csv"
            self.collector.export_to_csv(metrics_data, metrics_file)

            # Save benchmark output
            output_file = f"{self.output_dir}/{test_name}_{timestamp_str}_output.txt"
            with open(output_file, 'w') as f:
                f.write("=== STDOUT ===\n")
                f.write(stdout)
                f.write("\n=== STDERR ===\n")
                f.write(stderr)

            # Calculate summary statistics
            summary = self._calculate_summary(metrics_data)
            summary['test_name'] = test_name
            summary['start_time'] = start_dt.isoformat()
            summary['end_time'] = end_dt.isoformat()
            summary['duration_seconds'] = end_time - start_time
            summary['metrics_file'] = metrics_file
            summary['output_file'] = output_file
            summary['success'] = process.returncode == 0

            # Print summary
            self._print_summary(summary)

            return summary

        except subprocess.TimeoutExpired:
            print("⚠ Benchmark process timed out", file=sys.stderr)
            process.kill()
            return {'test_name': test_name, 'success': False, 'error': 'timeout'}
        except Exception as e:
            print(f"✗ Error running benchmark: {e}", file=sys.stderr)
            return {'test_name': test_name, 'success': False, 'error': str(e)}

    def _calculate_summary(self, metrics_data: dict) -> dict:
        """Calculate summary statistics from metrics data."""
        summary = {}

        for metric_name, series in metrics_data.items():
            if series:
                values = [v for _, v in series]
                summary[f"{metric_name}_avg"] = sum(values) / len(values)
                summary[f"{metric_name}_max"] = max(values)
                summary[f"{metric_name}_min"] = min(values)

        return summary

    def _print_summary(self, summary: dict):
        """Print test summary."""
        print(f"\n{'='*70}")
        print("Test Summary:")
        print(f"{'='*70}")
        print(f"Test Name: {summary.get('test_name')}")
        print(f"Success: {summary.get('success')}")
        print(f"Duration: {summary.get('duration_seconds', 0)/60:.1f} minutes")
        print(f"\nKey Metrics (averages):")
        print(f"  CPU Usage: {summary.get('cpu_usage_percent_avg', 0):.2f}%")
        print(f"  JVM Heap Usage: {summary.get('jvm_heap_usage_percent_avg', 0):.2f}%")
        print(f"  JVM Heap Used: {summary.get('jvm_heap_used_mb_avg', 0):.2f} MB")
        print(f"  Messages In/sec: {summary.get('messages_in_per_sec_avg', 0):.2f}")
        print(f"  Bytes In/sec: {summary.get('bytes_in_per_sec_avg', 0):.2f}")
        print(f"  Produce Latency p99: {summary.get('produce_latency_p99_ms_avg', 0):.2f} ms")
        print(f"  Fetch Latency p99: {summary.get('fetch_latency_p99_ms_avg', 0):.2f} ms")
        print(f"\nOutput files:")
        print(f"  Metrics: {summary.get('metrics_file')}")
        print(f"  Benchmark output: {summary.get('output_file')}")
        print(f"{'='*70}\n")

    def run_test_suite(self, num_runs: int = 1, cooldown_minutes: int = 10):
        """
        Run a complete test suite with multiple scenarios.

        Args:
            num_runs: Number of times to run each test (paper uses 10)
            cooldown_minutes: JVM cooldown between tests (paper uses 10)
        """
        # Define test scenarios
        test_scenarios = [
            {"name": "baseline_1p_1c", "producers": 1, "consumers": 1, "duration": 5},
            {"name": "scale_5p_5c", "producers": 5, "consumers": 5, "duration": 5},
            {"name": "producer_heavy_10p_2c", "producers": 10, "consumers": 2, "duration": 5},
            {"name": "consumer_heavy_2p_10c", "producers": 2, "consumers": 10, "duration": 5},
            {"name": "high_load_20p_20c", "producers": 20, "consumers": 20, "duration": 5},
        ]

        all_results = []

        print(f"\n{'#'*70}")
        print(f"# Benchmark Test Suite")
        print(f"# Total scenarios: {len(test_scenarios)}")
        print(f"# Runs per scenario: {num_runs}")
        print(f"# Cooldown between tests: {cooldown_minutes} minutes")
        print(f"# Output directory: {self.output_dir}")
        print(f"{'#'*70}\n")

        # Wait for Kafka to be ready
        self.wait_for_kafka_ready()

        for scenario_idx, scenario in enumerate(test_scenarios, 1):
            print(f"\n{'#'*70}")
            print(f"# Scenario {scenario_idx}/{len(test_scenarios)}: {scenario['name']}")
            print(f"{'#'*70}")

            for run in range(1, num_runs + 1):
                print(f"\n>>> Run {run}/{num_runs} for {scenario['name']}")

                # Run test with unique name
                test_name = f"{scenario['name']}_run{run}"
                result = self.run_single_benchmark(
                    test_name=test_name,
                    producers=scenario['producers'],
                    consumers=scenario['consumers'],
                    duration_minutes=scenario['duration']
                )

                all_results.append(result)

                # Cooldown between tests (except after last test)
                if not (scenario_idx == len(test_scenarios) and run == num_runs):
                    self.jvm_cooldown(cooldown_minutes * 60)

        # Save aggregated results
        self._save_aggregated_results(all_results)

        print(f"\n{'#'*70}")
        print(f"# Test Suite Complete!")
        print(f"# Total tests run: {len(all_results)}")
        print(f"# Results directory: {self.output_dir}")
        print(f"{'#'*70}\n")

    def _save_aggregated_results(self, results: list):
        """Save aggregated results to CSV."""
        import csv

        output_file = f"{self.output_dir}/aggregated_results.csv"

        if not results:
            return

        # Get all keys
        all_keys = set()
        for r in results:
            all_keys.update(r.keys())

        keys = sorted(all_keys)

        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(results)

        print(f"\n✓ Saved aggregated results to {output_file}")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run Kafka benchmarks with integrated metrics collection"
    )
    parser.add_argument(
        "--workload",
        default="./driver-kafka/kafka-test.yaml",
        help="Path to workload configuration"
    )
    parser.add_argument(
        "--output-dir",
        default="./benchmark_results",
        help="Directory for output files"
    )
    parser.add_argument(
        "--num-runs",
        type=int,
        default=1,
        help="Number of times to run each test scenario (paper uses 10)"
    )
    parser.add_argument(
        "--cooldown",
        type=int,
        default=10,
        help="JVM cooldown time between tests in minutes (paper uses 10)"
    )
    parser.add_argument(
        "--prometheus-url",
        default="http://localhost:9090",
        help="Prometheus server URL"
    )

    args = parser.parse_args()

    runner = BenchmarkRunner(
        workload_dir=args.workload,
        output_dir=args.output_dir,
        prometheus_url=args.prometheus_url
    )

    runner.run_test_suite(
        num_runs=args.num_runs,
        cooldown_minutes=args.cooldown
    )


if __name__ == "__main__":
    main()
