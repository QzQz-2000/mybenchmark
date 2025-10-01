#!/usr/bin/env python3
"""Quick start example for Python OpenMessaging Benchmark."""

import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from benchmark.core.config import WorkloadConfig, DriverConfig, BenchmarkConfig
from benchmark.core.coordinator import BenchmarkCoordinator
from benchmark.utils.logging import setup_logging


async def run_quick_benchmark():
    """Run a quick benchmark example."""

    # Setup logging
    setup_logging(level="INFO", component="example")

    # Create workload configuration
    workload_config = WorkloadConfig(
        name="Quick Start Example",
        topics=1,
        partitionsPerTopic=1,
        messageSize=1024,
        producersPerTopic=1,
        producerRate=1000,  # Low rate for quick demo
        consumerPerSubscription=1,
        testDurationMinutes=2,  # Short test
        warmupDurationMinutes=0  # Skip warmup for quick demo
    )

    # Create driver configuration
    driver_config = DriverConfig(
        name="Kafka",
        driverClass="benchmark.drivers.kafka.KafkaDriver",
        commonConfig={
            "bootstrap.servers": "localhost:9092"
        },
        producerConfig={
            "acks": "all",
            "linger.ms": "10",
            "batch.size": "16384"
        },
        consumerConfig={
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false"
        }
    )

    # Create benchmark configuration
    benchmark_config = BenchmarkConfig(
        workers=["http://localhost:8080"],  # Assumes one worker is running
        results_dir="results",
        enable_monitoring=True
    )

    print("🚀 Starting quick benchmark example...")
    print(f"   Workload: {workload_config.name}")
    print(f"   Duration: {workload_config.test_duration_minutes} minutes")
    print(f"   Rate: {workload_config.producer_rate} msg/s")
    print(f"   Workers: {len(benchmark_config.workers)}")

    try:
        # Run benchmark
        async with BenchmarkCoordinator(benchmark_config) as coordinator:
            result = await coordinator.run_benchmark(
                workload_config=workload_config,
                driver_config=driver_config
            )

            # Display results
            print("\n📊 Benchmark Results:")
            print(f"   Test ID: {result.test_id}")
            print(f"   Duration: {result.duration_seconds:.1f}s")

            if result.producer_stats:
                print(f"   Messages Sent: {result.producer_stats.total_messages:,}")
                print(f"   Producer Throughput: {result.producer_stats.messages_per_second:.0f} msg/s")

            if result.consumer_stats:
                print(f"   Messages Consumed: {result.consumer_stats.total_messages:,}")
                print(f"   Consumer Throughput: {result.consumer_stats.messages_per_second:.0f} msg/s")

            if result.latency_stats:
                print(f"   Avg Latency: {result.latency_stats.mean_ms:.2f}ms")
                print(f"   P99 Latency: {result.latency_stats.p99_ms:.2f}ms")

            # Save results
            results_file = Path("results") / f"quick_start_{result.test_id}.json"
            coordinator.result_collector.save_result(result, results_file)
            print(f"\n💾 Results saved to: {results_file}")

            print("\n✅ Quick start example completed successfully!")

    except Exception as e:
        print(f"\n❌ Benchmark failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Make sure Kafka is running on localhost:9092")
        print("2. Start a worker with: python scripts/start_worker.py kafka --worker-id worker-1 --port 8080")
        print("3. Check worker health: curl http://localhost:8080/health")
        sys.exit(1)


if __name__ == "__main__":
    print("Python OpenMessaging Benchmark - Quick Start Example")
    print("=" * 60)

    # Check if running as main script
    try:
        asyncio.run(run_quick_benchmark())
    except KeyboardInterrupt:
        print("\n⏹️  Benchmark interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)