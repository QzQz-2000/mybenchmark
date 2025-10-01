#!/usr/bin/env python3
"""
测试多进程 Worker 实现

每个 producer/consumer 运行在独立进程中，真实模拟多 agent 负载。
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from benchmark.core.coordinator import BenchmarkCoordinator
from benchmark.core.config import ConfigLoader, BenchmarkConfig


async def test_multiprocess_producers(num_producers: int):
    """测试指定数量的 producer"""

    print(f"\n{'='*80}")
    print(f"测试场景：{num_producers} 个 Producer（每个独立进程）")
    print(f"{'='*80}\n")

    # 加载配置
    workload_config = ConfigLoader.load_workload(
        f"workloads/test-{num_producers}-producers.yaml"
    )
    driver_config = ConfigLoader.load_driver("configs/kafka-digital-twin.yaml")

    # 创建 Coordinator
    benchmark_config = BenchmarkConfig(
        workers=["http://localhost:8001"],
        results_dir="./results",
        enable_monitoring=True,
        warmup_enabled=False,  # 为了快速测试
        cleanup_enabled=True
    )

    coordinator = BenchmarkCoordinator(benchmark_config)

    # 运行测试
    async with coordinator:
        result = await coordinator.run_benchmark(
            workload_config,
            driver_config,
            payload_data=None
        )

    # 打印结果
    print(f"\n{'='*80}")
    print(f"测试结果：{num_producers} 个 Producer")
    print(f"{'='*80}")
    print(f"Producer 吞吐量: {result.producer_stats.messages_per_second:.2f} msg/s")
    print(f"Producer 总消息: {result.producer_stats.total_messages}")
    print(f"Consumer 吞吐量: {result.consumer_stats.messages_per_second:.2f} msg/s")
    print(f"Consumer 总消息: {result.consumer_stats.total_messages}")
    print(f"延迟 p50: {result.latency_stats.p50_ms:.2f} ms")
    print(f"延迟 p95: {result.latency_stats.p95_ms:.2f} ms")
    print(f"延迟 p99: {result.latency_stats.p99_ms:.2f} ms")
    print(f"CPU 平均: {result.system_stats.cpu['avg']:.1f}%")
    print(f"CPU 最大: {result.system_stats.cpu['max']:.1f}%")
    print(f"内存平均: {result.system_stats.memory['avg']:.1f}%")
    print(f"{'='*80}\n")

    return result


async def run_scaling_test():
    """运行扩展性测试"""

    print("\n" + "="*80)
    print("Multi-Process Worker 扩展性测试")
    print("每个 producer 运行在独立进程中，测试扩展性")
    print("="*80)

    results = {}

    # 测试不同数量的 producer
    for num_producers in [1, 5, 10]:
        try:
            result = await test_multiprocess_producers(num_producers)
            results[num_producers] = result
        except Exception as e:
            print(f"❌ 测试 {num_producers} 个 producer 失败: {e}")
            import traceback
            traceback.print_exc()

    # 对比分析
    print("\n" + "="*80)
    print("扩展性分析对比")
    print("="*80)
    print(f"{'Producers':<12} {'吞吐量(msg/s)':<20} {'延迟p99(ms)':<15} {'CPU平均(%)':<15}")
    print("-"*80)

    for num_producers in [1, 5, 10]:
        if num_producers in results:
            r = results[num_producers]
            print(
                f"{num_producers:<12} "
                f"{r.producer_stats.messages_per_second:<20.2f} "
                f"{r.latency_stats.p99_ms:<15.2f} "
                f"{r.system_stats.cpu['avg']:<15.1f}"
            )

    print("="*80)

    # 计算扩展效率
    if 1 in results and 10 in results:
        baseline = results[1].producer_stats.messages_per_second
        scaled = results[10].producer_stats.messages_per_second
        efficiency = (scaled / baseline / 10) * 100

        print(f"\n扩展效率:")
        print(f"  1 producer: {baseline:.2f} msg/s")
        print(f"  10 producers: {scaled:.2f} msg/s")
        print(f"  理想值: {baseline * 10:.2f} msg/s (10x)")
        print(f"  实际扩展: {scaled / baseline:.2f}x")
        print(f"  扩展效率: {efficiency:.1f}%")

        if efficiency > 90:
            print("  ✅ 扩展性优秀！接近线性扩展")
        elif efficiency > 70:
            print("  👍 扩展性良好")
        else:
            print("  ⚠️ 扩展性一般，可能存在瓶颈")


if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║                Multi-Process Worker 测试工具                          ║
╚══════════════════════════════════════════════════════════════════════╝

使用前准备:
1. 启动 Kafka:
   docker-compose up -d kafka zookeeper

2. 启动 Multi-Process Worker:
   python workers/kafka_worker_multiprocess.py \\
       --worker-id worker-1 \\
       --port 8001 \\
       --driver-config configs/kafka-digital-twin.yaml

3. 创建测试工作负载配置（如果不存在）

现在开始测试...
    """)

    try:
        asyncio.run(run_scaling_test())
    except KeyboardInterrupt:
        print("\n\n⚠️ 测试被用户中断")
    except Exception as e:
        print(f"\n\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
