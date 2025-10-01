#!/usr/bin/env python3
"""简单测试 - 验证多进程 Worker 基本功能"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from benchmark.core.coordinator import BenchmarkCoordinator
from benchmark.core.config import ConfigLoader, BenchmarkConfig


async def simple_test():
    """简单测试 1 个 producer"""

    print("\n" + "="*80)
    print("简单功能测试：1 个 Producer（独立进程）")
    print("="*80 + "\n")

    # 加载配置
    workload_config = ConfigLoader.load_workload("workloads/test-1-producers.yaml")
    driver_config = ConfigLoader.load_driver("configs/kafka-digital-twin.yaml")

    # 创建 Coordinator
    benchmark_config = BenchmarkConfig(
        workers=["http://localhost:8001"],
        results_dir="./results",
        enable_monitoring=True,
        warmup_enabled=False,
        cleanup_enabled=True
    )

    coordinator = BenchmarkCoordinator(benchmark_config)

    # 运行测试
    print("🚀 开始测试...\n")
    async with coordinator:
        result = await coordinator.run_benchmark(
            workload_config,
            driver_config,
            payload_data=None
        )

    # 打印结果
    print("\n" + "="*80)
    print("✅ 测试完成")
    print("="*80)

    if result:
        print(f"Producer 吞吐量: {result.producer_stats.messages_per_second:.2f} msg/s")
        print(f"Producer 总消息: {result.producer_stats.total_messages}")
        print(f"Consumer 吞吐量: {result.consumer_stats.messages_per_second:.2f} msg/s")
        print(f"Consumer 总消息: {result.consumer_stats.total_messages}")

        if result.latency_stats:
            print(f"延迟 p50: {result.latency_stats.p50_ms:.2f} ms")
            print(f"延迟 p95: {result.latency_stats.p95_ms:.2f} ms")
            print(f"延迟 p99: {result.latency_stats.p99_ms:.2f} ms")

        if result.system_stats:
            print(f"CPU 平均: {result.system_stats.cpu.get('avg', 0):.1f}%")
            print(f"内存平均: {result.system_stats.memory.get('avg', 0):.1f}%")
    else:
        print("⚠️  测试未返回结果")

    print("="*80 + "\n")

    return result


if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║            Multi-Process Worker 简单功能测试                          ║
╚══════════════════════════════════════════════════════════════════════╝

测试内容：
- 1 个 Producer（独立进程）
- 4 个 Consumer（独立进程）
- 验证基本功能是否正常

准备工作：
✅ Kafka 应该已启动
✅ Worker 应该已运行在 http://localhost:8001

开始测试...
    """)

    try:
        asyncio.run(simple_test())
        print("\n✅ 测试成功！多进程模式工作正常。\n")
    except KeyboardInterrupt:
        print("\n\n⚠️ 测试被用户中断")
    except Exception as e:
        print(f"\n\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
