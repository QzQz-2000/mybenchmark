#!/usr/bin/env python3
"""Direct coordinator entry point."""

import sys
import asyncio
import click
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from benchmark.core.coordinator import BenchmarkCoordinator
from benchmark.core.config import ConfigLoader, BenchmarkConfig
from benchmark.utils.logging import setup_logging


@click.command()
@click.option('--workers', '-w', multiple=True, required=True,
              help='Worker URLs (e.g., http://localhost:8080)')
@click.option('--workload', '-wl', required=True,
              help='Workload configuration file')
@click.option('--driver', '-d',
              help='Driver configuration file')
@click.option('--output-dir', '-o', default='results',
              help='Output directory for results')
@click.option('--log-level', default='INFO',
              help='Logging level')
@click.option('--enable-monitoring', is_flag=True,
              help='Enable system monitoring')
def main(workers, workload, driver, output_dir, log_level, enable_monitoring):
    """Run benchmark coordinator."""

    # Setup logging
    setup_logging(level=log_level, component="coordinator")

    async def run_benchmark():
        """Run the benchmark asynchronously."""
        try:
            # Load configurations
            workload_config = ConfigLoader.load_workload(workload)

            driver_config = None
            if driver:
                driver_config = ConfigLoader.load_driver(driver)

            # Create benchmark config
            benchmark_config = BenchmarkConfig(
                workers=list(workers),
                results_dir=output_dir,
                enable_monitoring=enable_monitoring,
                log_level=log_level
            )

            print(f"🚀 Starting benchmark: {workload_config.name}")
            print(f"   Workers: {len(workers)}")
            print(f"   Duration: {workload_config.test_duration_minutes} minutes")

            # Run benchmark
            async with BenchmarkCoordinator(benchmark_config) as coordinator:
                result = await coordinator.run_benchmark(
                    workload_config=workload_config,
                    driver_config=driver_config
                )

                # Save results to file
                import os
                import json
                from pathlib import Path
                from datetime import datetime

                # Create results directory
                results_dir = Path(output_dir)
                results_dir.mkdir(exist_ok=True)

                # Create filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                # Clean filename for filesystem compatibility
                import re
                clean_name = re.sub(r'[^A-Za-z0-9._-]', '_', workload_config.name)
                filename = f"{clean_name}_{timestamp}.json"
                results_file = results_dir / filename

                # Convert result to dict for JSON serialization
                result_dict = {
                    "test_id": result.test_id,
                    "test_name": result.test_name,
                    "start_time": result.start_time,
                    "end_time": result.end_time,
                    "duration_seconds": result.duration_seconds,
                    "workload_config": result.workload_config,
                    "driver_config": result.driver_config,
                    "producer_results": [
                        {
                            "worker_id": pr.worker_id,
                            "worker_url": pr.worker_url,
                            "task_type": pr.task_type,
                            "start_time": pr.start_time,
                            "end_time": pr.end_time,
                            "duration_seconds": pr.duration_seconds,
                            "throughput": {
                                "total_messages": pr.throughput.total_messages,
                                "total_bytes": pr.throughput.total_bytes,
                                "duration_seconds": pr.throughput.duration_seconds,
                                "messages_per_second": pr.throughput.messages_per_second,
                                "bytes_per_second": pr.throughput.bytes_per_second,
                                "mb_per_second": pr.throughput.mb_per_second
                            },
                            "latency": {
                                "min_ms": pr.latency.min_ms if pr.latency else 0,
                                "max_ms": pr.latency.max_ms if pr.latency else 0,
                                "mean_ms": pr.latency.mean_ms if pr.latency else 0,
                                "median_ms": pr.latency.median_ms if pr.latency else 0,
                                "p50_ms": pr.latency.p50_ms if pr.latency else 0,
                                "p95_ms": pr.latency.p95_ms if pr.latency else 0,
                                "p99_ms": pr.latency.p99_ms if pr.latency else 0,
                                "p99_9_ms": pr.latency.p99_9_ms if pr.latency else 0
                            } if pr.latency else {},
                            "errors": {
                                "total_errors": pr.errors.total_errors,
                                "error_rate": pr.errors.error_rate,
                                "error_types": pr.errors.error_types
                            }
                        } for pr in result.producer_results
                    ],
                    "consumer_results": [
                        {
                            "worker_id": cr.worker_id,
                            "worker_url": cr.worker_url,
                            "task_type": cr.task_type,
                            "start_time": cr.start_time,
                            "end_time": cr.end_time,
                            "duration_seconds": cr.duration_seconds,
                            "throughput": {
                                "total_messages": cr.throughput.total_messages,
                                "total_bytes": cr.throughput.total_bytes,
                                "duration_seconds": cr.throughput.duration_seconds,
                                "messages_per_second": cr.throughput.messages_per_second,
                                "bytes_per_second": cr.throughput.bytes_per_second,
                                "mb_per_second": cr.throughput.mb_per_second
                            },
                            "errors": {
                                "total_errors": cr.errors.total_errors,
                                "error_rate": cr.errors.error_rate,
                                "error_types": cr.errors.error_types
                            }
                        } for cr in result.consumer_results
                    ]
                }

                # Write to file
                with open(results_file, 'w') as f:
                    json.dump(result_dict, f, indent=2)

                print(f"\n✅ Benchmark completed: {result.test_id}")
                print(f"   Results saved to: {results_file}")

                # Display detailed results
                print("\n📊 **TEST RESULTS SUMMARY**")
                print("=" * 60)

                # Calculate totals from producer and consumer results
                total_producer_msgs = 0
                total_consumer_msgs = 0
                producer_throughput = 0
                consumer_throughput = 0
                avg_producer_latency = 0
                producer_errors = 0
                consumer_errors = 0

                # Process producer results
                if hasattr(result, 'producer_results') and result.producer_results:
                    for prod_result in result.producer_results:
                        if hasattr(prod_result, 'throughput') and prod_result.throughput:
                            total_producer_msgs += prod_result.throughput.total_messages
                            producer_throughput += prod_result.throughput.messages_per_second

                        if hasattr(prod_result, 'latency') and prod_result.latency:
                            avg_producer_latency += prod_result.latency.mean_ms

                        if hasattr(prod_result, 'errors') and prod_result.errors:
                            producer_errors += prod_result.errors.total_errors

                # Process consumer results
                if hasattr(result, 'consumer_results') and result.consumer_results:
                    for cons_result in result.consumer_results:
                        if hasattr(cons_result, 'throughput') and cons_result.throughput:
                            total_consumer_msgs += cons_result.throughput.total_messages
                            consumer_throughput += cons_result.throughput.messages_per_second

                        if hasattr(cons_result, 'errors') and cons_result.errors:
                            consumer_errors += cons_result.errors.total_errors

                # Display results
                print(f"🚀 Producer Messages Sent: {total_producer_msgs:,}")
                print(f"📥 Consumer Messages Received: {total_consumer_msgs:,}")
                print(f"⚡ Producer Throughput: {producer_throughput:.2f} messages/sec")
                print(f"⚡ Consumer Throughput: {consumer_throughput:.2f} messages/sec")

                if avg_producer_latency > 0 and len(result.producer_results) > 0:
                    print(f"⏱️  Average Producer Latency: {avg_producer_latency/len(result.producer_results):.2f} ms")

                total_errors = producer_errors + consumer_errors
                print(f"❌ Total Errors: {total_errors}")

                # Show aggregated stats if available
                if hasattr(result, 'producer_stats') and result.producer_stats:
                    print(f"📊 Aggregated Producer Stats: {result.producer_stats.messages_per_second:.2f} msg/s")

                if hasattr(result, 'consumer_stats') and result.consumer_stats:
                    print(f"📊 Aggregated Consumer Stats: {result.consumer_stats.messages_per_second:.2f} msg/s")

                # Test configuration
                print(f"\n🔧 Test Configuration:")
                print(f"   • Duration: {workload_config.test_duration_minutes} minutes")
                print(f"   • Message Size: {workload_config.message_size} bytes")
                print(f"   • Topics: {workload_config.topics}")
                print(f"   • Partitions per Topic: {workload_config.partitions_per_topic}")
                print(f"   • Producers per Topic: {workload_config.producers_per_topic}")
                print(f"   • Producer Rate: {workload_config.producer_rate} msg/s")
                print(f"   • Consumers per Subscription: {workload_config.consumer_per_subscription}")

                # Debug info
                print(f"\n🔍 Debug Info:")
                print(f"   • Producer Results Count: {len(result.producer_results) if hasattr(result, 'producer_results') else 0}")
                print(f"   • Consumer Results Count: {len(result.consumer_results) if hasattr(result, 'consumer_results') else 0}")

                print("=" * 60)

        except Exception as e:
            print(f"❌ Benchmark failed: {e}")
            sys.exit(1)

    # Run the async function
    asyncio.run(run_benchmark())


if __name__ == '__main__':
    main()