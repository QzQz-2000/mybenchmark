#!/usr/bin/env python3
"""快速测试 - 1分钟验证多进程功能"""

import asyncio
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from benchmark.core.coordinator import BenchmarkCoordinator
from benchmark.core.config import ConfigLoader, BenchmarkConfig


async def quick_test():
    """快速测试 1 个 producer"""

    print("\n" + "="*80)
    print("快速测试：1 Producer + 1 Consumer（各独立进程，1分钟）")
    print("="*80 + "\n")

    workload_config = ConfigLoader.load_workload("workloads/test-quick-1-producer.yaml")
    driver_config = ConfigLoader.load_driver("configs/kafka-digital-twin.yaml")

    benchmark_config = BenchmarkConfig(
        workers=["http://localhost:8001"],
        results_dir="./results",
        enable_monitoring=True,
        warmup_enabled=False,
        cleanup_enabled=True
    )

    coordinator = BenchmarkCoordinator(benchmark_config)

    print("🚀 开始测试...\n")
    async with coordinator:
        result = await coordinator.run_benchmark(
            workload_config,
            driver_config,
            payload_data=None
        )

    print("\n" + "="*80)
    print("✅ 测试完成")
    print("="*80)

    if result and hasattr(result, 'producer_stats') and result.producer_stats and result.producer_stats.total_messages > 0:
        print(f"Producer 吞吐量: {result.producer_stats.messages_per_second:.2f} msg/s")
        print(f"Producer 总消息: {result.producer_stats.total_messages}")
        print(f"Consumer 吞吐量: {result.consumer_stats.messages_per_second:.2f} msg/s")
        print(f"Consumer 总消息: {result.consumer_stats.total_messages}")

        if result.latency_stats:
            print(f"延迟 p50: {result.latency_stats.p50_ms:.2f} ms")
            print(f"延迟 p99: {result.latency_stats.p99_ms:.2f} ms")

        print("\n✅ 多进程模式工作正常！")
    else:
        print("⚠️  测试未返回有效结果")

    print("="*80 + "\n")

    return result


if __name__ == "__main__":
    try:
        asyncio.run(quick_test())
    except KeyboardInterrupt:
        print("\n\n⚠️ 测试被用户中断")
    except Exception as e:
        print(f"\n\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
