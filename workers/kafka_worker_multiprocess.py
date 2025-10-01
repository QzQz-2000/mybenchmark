"""
Kafka Worker with Multi-Process Execution

每个 Producer/Consumer 任务运行在独立进程中，真实模拟多 agent 场景。
这个实现完全对应 Java OMB 的多线程模型，但使用 Python 多进程绕过 GIL。
"""

import time
import asyncio
from typing import Dict, List, Any, Optional

from benchmark.core.worker import BaseWorker, ProducerTask, ConsumerTask
from benchmark.core.results import WorkerResult
from benchmark.core.config import DriverConfig
from benchmark.worker.process_executor import ProcessExecutor
from benchmark.drivers.kafka import KafkaDriver
from benchmark.utils.logging import LoggerMixin


class KafkaWorkerMultiProcess(BaseWorker):
    """
    Kafka Worker - Multi-Process 版本

    每个 producer/consumer 任务运行在独立进程中，完全模拟真实的多 agent 负载。

    架构：
    - 单个 Worker 进程（FastAPI 服务器）
    - N 个 Producer 子进程（每个任务一个进程）
    - M 个 Consumer 子进程（每个任务一个进程）

    对应 Java OMB:
    - 单个 JVM 进程 → 单个 Worker 进程
    - N 个线程 → N 个子进程
    - 多线程并发 → 多进程并发
    """

    def __init__(
        self,
        worker_id: str,
        driver_config: DriverConfig
    ):
        """
        初始化 Kafka Worker

        Args:
            worker_id: Worker 唯一标识
            driver_config: Kafka 驱动配置
        """
        super().__init__(worker_id)
        self.driver_config = driver_config
        self.client_type = "confluent-kafka"
        self._driver: Optional[KafkaDriver] = None
        self._process_executor: Optional[ProcessExecutor] = None

    async def start(self) -> None:
        """启动 Worker"""
        await super().start()

        # 初始化 Driver（用于 topic 管理等）
        self._driver = KafkaDriver(self.driver_config)
        await self._driver.initialize()

        # 创建进程执行器
        self._process_executor = ProcessExecutor(self.worker_id)

        self.logger.info(
            f"✅ Kafka Worker {self.worker_id} 已启动 (Multi-Process 模式)"
        )

    async def stop(self) -> None:
        """停止 Worker"""
        # 清理进程执行器
        if self._process_executor:
            self._process_executor.cleanup()

        # 清理 Driver
        if self._driver:
            await self._driver.cleanup()

        await super().stop()

        self.logger.info(f"🛑 Kafka Worker {self.worker_id} 已停止")

    async def run_producer_tasks(
        self,
        tasks: List[ProducerTask]
    ) -> List[WorkerResult]:
        """
        运行多个 producer 任务

        每个任务运行在独立进程中，真实模拟多 agent 并发负载。

        Args:
            tasks: Producer 任务列表

        Returns:
            所有任务的执行结果
        """
        self.logger.info(
            f"📤 收到 {len(tasks)} 个 Producer 任务，"
            f"每个任务将运行在独立进程中..."
        )

        # 使用进程执行器运行所有任务
        results = await self._process_executor.execute_producer_tasks(
            tasks,
            self.driver_config
        )

        # 统计
        total_messages = sum(r.throughput.total_messages for r in results)
        total_rate = sum(r.throughput.messages_per_second for r in results)

        self.logger.info(
            f"✅ 所有 {len(tasks)} 个 Producer 任务完成: "
            f"{total_messages} 条消息, "
            f"{total_rate:.1f} msg/s"
        )

        return results

    async def run_consumer_tasks(
        self,
        tasks: List[ConsumerTask]
    ) -> List[WorkerResult]:
        """
        运行多个 consumer 任务

        每个任务运行在独立进程中，真实模拟多 agent 并发负载。

        Args:
            tasks: Consumer 任务列表

        Returns:
            所有任务的执行结果
        """
        self.logger.info(
            f"📥 收到 {len(tasks)} 个 Consumer 任务，"
            f"每个任务将运行在独立进程中..."
        )

        # 使用进程执行器运行所有任务
        results = await self._process_executor.execute_consumer_tasks(
            tasks,
            self.driver_config
        )

        # 统计
        total_messages = sum(r.throughput.total_messages for r in results)
        total_rate = sum(r.throughput.messages_per_second for r in results)

        self.logger.info(
            f"✅ 所有 {len(tasks)} 个 Consumer 任务完成: "
            f"{total_messages} 条消息, "
            f"{total_rate:.1f} msg/s"
        )

        return results

    async def _execute_producer_task(self, task: ProducerTask) -> Dict[str, Any]:
        """
        执行单个 producer 任务（内部方法）

        注意：这个方法不应该被直接调用，因为我们覆盖了 run_producer_tasks。
        保留它是为了兼容 BaseWorker 接口。
        """
        raise NotImplementedError(
            "KafkaWorkerMultiProcess 使用 run_producer_tasks，不应调用此方法"
        )

    async def _execute_consumer_task(self, task: ConsumerTask) -> Dict[str, Any]:
        """
        执行单个 consumer 任务（内部方法）

        注意：这个方法不应该被直接调用，因为我们覆盖了 run_consumer_tasks。
        保留它是为了兼容 BaseWorker 接口。
        """
        raise NotImplementedError(
            "KafkaWorkerMultiProcess 使用 run_consumer_tasks，不应调用此方法"
        )

    def get_client_info(self) -> Dict[str, Any]:
        """获取客户端信息"""
        info = {
            'worker_id': self.worker_id,
            'client_type': self.client_type,
            'driver_name': self.driver_config.name if self.driver_config else 'unknown',
            'execution_mode': 'multi-process',
            'description': 'Each producer/consumer runs in independent process'
        }

        if self._driver:
            info.update(self._driver.get_client_info())

        return info


# ============================================================================
# Worker 启动脚本
# ============================================================================

if __name__ == "__main__":
    import argparse
    import sys
    from pathlib import Path

    # Add project root to path
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))

    from benchmark.api.worker_api import run_worker_server
    from benchmark.core.config import ConfigLoader, DriverConfig

    parser = argparse.ArgumentParser(
        description='Kafka Worker (Multi-Process Mode)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:

# 启动 Worker (自动使用多进程模式)
python workers/kafka_worker_multiprocess.py \\
    --worker-id worker-1 \\
    --port 8001 \\
    --driver-config configs/kafka-digital-twin.yaml

# 测试 10 个 producer (每个独立进程)
py-omb-coordinator \\
    --workload workloads/test-10-producers.yaml \\
    --driver configs/kafka-digital-twin.yaml \\
    --workers http://localhost:8001

特点:
- 每个 producer/consumer 运行在独立进程
- 真实模拟多 agent 负载场景
- 完全绕过 GIL，充分利用多核
- 对应 Java OMB 的多线程模型
        """
    )

    parser.add_argument('--port', type=int, default=8001, help='服务端口')
    parser.add_argument('--host', default='0.0.0.0', help='绑定地址')
    parser.add_argument('--worker-id', default='multiprocess-worker-1', help='Worker ID')
    parser.add_argument('--driver-config', required=True, help='Driver 配置文件路径')

    args = parser.parse_args()

    # 加载 driver 配置
    driver_config = ConfigLoader.load_driver(args.driver_config)

    # 创建 worker 实例
    worker = KafkaWorkerMultiProcess(
        worker_id=args.worker_id,
        driver_config=driver_config
    )

    print("=" * 80)
    print("🚀 Kafka Worker (Multi-Process Mode)")
    print("=" * 80)
    print(f"   Worker ID: {args.worker_id}")
    print(f"   Client: Confluent Kafka")
    print(f"   Execution: Multi-Process (每个任务独立进程)")
    print(f"   Port: {args.port}")
    print(f"   Health URL: http://{args.host}:{args.port}/health")
    print("=" * 80)
    print()
    print("✨ 特点:")
    print("   - 每个 producer/consumer 独立进程")
    print("   - 真实模拟多 agent 负载")
    print("   - 绕过 GIL，充分利用多核")
    print("   - 测试结果准确可靠")
    print("=" * 80)
    print()

    # 启动服务器
    asyncio.run(run_worker_server(worker, host=args.host, port=args.port))
