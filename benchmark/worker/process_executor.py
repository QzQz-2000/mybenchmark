"""
多进程执行器 - 每个 Producer/Consumer 独立进程运行

这个模块实现了类似 Java OMB 的多线程模型，但使用 Python 多进程来绕过 GIL。
每个 producer/consumer agent 运行在独立的进程中，真实模拟多 agent 负载场景。
"""

import multiprocessing as mp
import asyncio
import time
import sys
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
import traceback
import json

from benchmark.core.worker import ProducerTask, ConsumerTask
from benchmark.core.config import DriverConfig
from benchmark.core.results import WorkerResult, ThroughputStats, LatencyStats, ErrorStats
from benchmark.utils.logging import LoggerMixin


@dataclass
class ProcessResult:
    """进程执行结果"""
    task_id: str
    task_type: str  # 'producer' or 'consumer'
    pid: int
    start_time: float
    end_time: float
    success: bool

    # 统计数据
    messages_sent: int = 0
    messages_received: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    errors: int = 0

    # 延迟统计 (dict 格式便于序列化)
    latency_stats: Optional[Dict] = None

    # 错误信息
    error_message: Optional[str] = None

    def to_dict(self) -> Dict:
        """转换为字典"""
        return asdict(self)


class ProcessExecutor(LoggerMixin):
    """
    多进程执行器

    负责管理多个独立的 producer/consumer 进程，每个任务运行在独立进程中。
    """

    def __init__(self, worker_id: str):
        super().__init__()
        self.worker_id = worker_id
        self._processes: List[mp.Process] = []
        self._result_queue = mp.Queue()

    async def execute_producer_tasks(
        self,
        tasks: List[ProducerTask],
        driver_config: DriverConfig
    ) -> List[WorkerResult]:
        """
        执行多个 producer 任务，每个任务一个独立进程

        Args:
            tasks: Producer 任务列表
            driver_config: Driver 配置

        Returns:
            所有 producer 的执行结果
        """
        # 清空之前的进程列表（确保不累积）
        self._processes.clear()

        self.logger.info(f"🚀 启动 {len(tasks)} 个独立 Producer 进程...")

        # 启动所有 producer 进程
        for task in tasks:
            process = mp.Process(
                target=_producer_process_main,
                args=(task, driver_config, self._result_queue),
                name=f"producer-{task.task_id}"
            )
            process.start()
            self._processes.append(process)

            self.logger.info(
                f"   ✅ 启动 Producer: {task.task_id} (PID: {process.pid})"
            )

        # 等待所有进程完成
        results = await self._wait_for_processes(len(tasks))

        # 转换为 WorkerResult
        worker_results = []
        for result in results:
            worker_result = self._convert_to_worker_result(result)
            worker_results.append(worker_result)

        self.logger.info(f"✨ 所有 {len(tasks)} 个 Producer 进程已完成")

        return worker_results

    async def execute_consumer_tasks(
        self,
        tasks: List[ConsumerTask],
        driver_config: DriverConfig
    ) -> List[WorkerResult]:
        """
        执行多个 consumer 任务，每个任务一个独立进程

        Args:
            tasks: Consumer 任务列表
            driver_config: Driver 配置

        Returns:
            所有 consumer 的执行结果
        """
        # 清空之前的进程列表（确保不累积）
        self._processes.clear()

        self.logger.info(f"🚀 启动 {len(tasks)} 个独立 Consumer 进程...")

        # 启动所有 consumer 进程
        for task in tasks:
            process = mp.Process(
                target=_consumer_process_main,
                args=(task, driver_config, self._result_queue),
                name=f"consumer-{task.task_id}"
            )
            process.start()
            self._processes.append(process)

            self.logger.info(
                f"   ✅ 启动 Consumer: {task.task_id} (PID: {process.pid})"
            )

        # 等待所有进程完成
        results = await self._wait_for_processes(len(tasks))

        # 转换为 WorkerResult
        worker_results = []
        for result in results:
            worker_result = self._convert_to_worker_result(result)
            worker_results.append(worker_result)

        self.logger.info(f"✨ 所有 {len(tasks)} 个 Consumer 进程已完成")

        return worker_results

    async def _wait_for_processes(self, expected_count: int) -> List[ProcessResult]:
        """
        等待所有进程完成并收集结果

        Args:
            expected_count: 期望的结果数量

        Returns:
            所有进程的结果
        """
        results = []

        # 等待所有进程
        for process in self._processes:
            process.join()

        # 收集所有结果
        while not self._result_queue.empty():
            try:
                result_dict = self._result_queue.get_nowait()
                result = ProcessResult(**result_dict)
                results.append(result)
            except Exception as e:
                self.logger.error(f"收集结果失败: {e}")

        if len(results) != expected_count:
            self.logger.warning(
                f"期望 {expected_count} 个结果，实际收到 {len(results)} 个"
            )

        return results

    def _convert_to_worker_result(self, process_result: ProcessResult) -> WorkerResult:
        """将 ProcessResult 转换为 WorkerResult"""

        # 构建 ThroughputStats
        duration = process_result.end_time - process_result.start_time
        if process_result.task_type == 'producer':
            throughput = ThroughputStats(
                total_messages=process_result.messages_sent,
                total_bytes=process_result.bytes_sent,
                duration_seconds=duration,
                messages_per_second=process_result.messages_sent / duration if duration > 0 else 0,
                bytes_per_second=process_result.bytes_sent / duration if duration > 0 else 0,
                mb_per_second=(process_result.bytes_sent / duration / 1024 / 1024) if duration > 0 else 0
            )
        else:
            throughput = ThroughputStats(
                total_messages=process_result.messages_received,
                total_bytes=process_result.bytes_received,
                duration_seconds=duration,
                messages_per_second=process_result.messages_received / duration if duration > 0 else 0,
                bytes_per_second=process_result.bytes_received / duration if duration > 0 else 0,
                mb_per_second=(process_result.bytes_received / duration / 1024 / 1024) if duration > 0 else 0
            )

        # 构建 LatencyStats
        if process_result.latency_stats:
            latency = LatencyStats(**process_result.latency_stats)
        else:
            latency = LatencyStats(
                count=0,
                min_ms=0, max_ms=0, mean_ms=0, median_ms=0,
                p50_ms=0, p95_ms=0, p99_ms=0, p99_9_ms=0
            )

        # 构建 ErrorStats
        error_rate = 0.0
        if process_result.task_type == 'producer' and process_result.messages_sent > 0:
            error_rate = process_result.errors / (process_result.messages_sent + process_result.errors)

        errors = ErrorStats(
            total_errors=process_result.errors,
            error_rate=error_rate,
            error_types={}
        )

        # 构建 WorkerResult
        return WorkerResult(
            worker_id=f"{self.worker_id}-process-{process_result.pid}",
            worker_url="local-process",
            task_type=process_result.task_type,
            start_time=process_result.start_time,
            end_time=process_result.end_time,
            throughput=throughput,
            latency=latency,
            errors=errors,
            metadata={
                'task_id': process_result.task_id,
                'pid': process_result.pid,
                'success': process_result.success,
                'error_message': process_result.error_message
            }
        )

    def cleanup(self):
        """清理资源"""
        # 确保所有进程已终止
        for process in self._processes:
            if process.is_alive():
                self.logger.warning(f"强制终止进程 {process.pid}")
                process.terminate()
                process.join(timeout=5)
                if process.is_alive():
                    process.kill()

        self._processes.clear()


# ============================================================================
# 进程入口函数（在子进程中运行）
# ============================================================================

def _producer_process_main(
    task: ProducerTask,
    driver_config: DriverConfig,
    result_queue: mp.Queue
):
    """
    Producer 进程主函数（运行在独立进程中）

    这是每个 producer agent 的入口点，模拟真实的独立应用。
    """
    try:
        # 在子进程中需要重新导入和初始化
        import asyncio

        # 运行异步逻辑
        result = asyncio.run(_run_producer_async(task, driver_config))

        # 返回结果
        result_queue.put(result.to_dict())

    except Exception as e:
        # 错误处理
        error_result = ProcessResult(
            task_id=task.task_id,
            task_type='producer',
            pid=mp.current_process().pid,
            start_time=time.time(),
            end_time=time.time(),
            success=False,
            error_message=f"{str(e)}\n{traceback.format_exc()}"
        )
        result_queue.put(error_result.to_dict())


def _consumer_process_main(
    task: ConsumerTask,
    driver_config: DriverConfig,
    result_queue: mp.Queue
):
    """
    Consumer 进程主函数（运行在独立进程中）

    这是每个 consumer agent 的入口点，模拟真实的独立应用。
    """
    try:
        # 在子进程中需要重新导入和初始化
        import asyncio

        # 运行异步逻辑
        result = asyncio.run(_run_consumer_async(task, driver_config))

        # 返回结果
        result_queue.put(result.to_dict())

    except Exception as e:
        # 错误处理
        error_result = ProcessResult(
            task_id=task.task_id,
            task_type='consumer',
            pid=mp.current_process().pid,
            start_time=time.time(),
            end_time=time.time(),
            success=False,
            error_message=f"{str(e)}\n{traceback.format_exc()}"
        )
        result_queue.put(error_result.to_dict())


async def _run_producer_async(
    task: ProducerTask,
    driver_config: DriverConfig
) -> ProcessResult:
    """
    异步运行 producer 逻辑（在子进程中）
    """
    from benchmark.drivers.kafka import KafkaDriver
    from benchmark.drivers.base import Message, DriverUtils
    from benchmark.utils.rate_limiter import create_rate_limiter

    pid = mp.current_process().pid
    print(f"[Producer {task.task_id} PID:{pid}] 🚀 启动", flush=True)

    start_time = time.time()
    messages_sent = 0
    bytes_sent = 0
    errors = 0

    try:
        # 1. 创建 Driver
        driver = KafkaDriver(driver_config)
        await driver.initialize()

        # 2. 创建 Producer
        producer = driver.create_producer()
        await producer._initialize_producer()

        # 3. 创建速率限制器
        rate_limiter = None
        if task.rate_limit and task.rate_limit > 0:
            rate_limiter = create_rate_limiter(task.rate_limit)

        # 4. 生成 payload
        payload = DriverUtils.create_test_payload(
            task.message_size,
            task.payload_data
        )

        # 5. 发送消息循环
        print(f"[Producer {task.task_id} PID:{pid}] 📤 开始发送 {task.num_messages} 条消息", flush=True)

        for i in range(task.num_messages):
            # 速率限制
            if rate_limiter:
                await rate_limiter.acquire()

            # 生成消息
            key = DriverUtils.generate_message_key(
                task.key_pattern, i, task.num_messages
            )

            send_timestamp_ms = int(time.time() * 1000)
            headers = {
                'send_timestamp': str(send_timestamp_ms).encode(),
                'task_id': task.task_id.encode(),
                'producer_pid': str(pid).encode()
            }

            message = Message(key, payload, headers, time.time())

            # 发送
            try:
                await producer.send_message(task.topic, message)
                messages_sent += 1
                bytes_sent += len(payload)
            except Exception as e:
                errors += 1
                if errors <= 10:  # 只打印前 10 个错误
                    print(f"[Producer {task.task_id} PID:{pid}] ❌ 发送失败: {e}", flush=True)

            # 进度日志
            if messages_sent > 0 and messages_sent % 10000 == 0:
                print(f"[Producer {task.task_id} PID:{pid}] 📊 已发送 {messages_sent}/{task.num_messages}", flush=True)

        # 6. 刷新
        print(f"[Producer {task.task_id} PID:{pid}] 🔄 刷新...", flush=True)
        await producer.flush()

        # 7. 等待交付确认
        print(f"[Producer {task.task_id} PID:{pid}] ⏳ 等待交付确认...", flush=True)
        delivery_status = await producer.wait_for_delivery(timeout_seconds=60)

        if delivery_status.get('timed_out'):
            pending = delivery_status.get('pending_messages', 0)
            print(f"[Producer {task.task_id} PID:{pid}] ⚠️ 超时: {pending} 条消息未确认", flush=True)

        # 8. 获取延迟统计
        latency_snapshot = producer.get_latency_snapshot()

        from benchmark.utils.latency_recorder import snapshot_to_legacy_stats
        latency_stats = snapshot_to_legacy_stats(
            latency_snapshot,
            producer._latency_recorder.export_histogram()
        )

        # 9. 关闭
        await producer.close()
        await driver.cleanup()

        end_time = time.time()
        duration = end_time - start_time

        print(
            f"[Producer {task.task_id} PID:{pid}] ✅ 完成: "
            f"{messages_sent} 条消息, "
            f"{messages_sent/duration:.1f} msg/s, "
            f"延迟 p99={latency_snapshot.p99_ms:.2f}ms",
            flush=True
        )

        # 10. 返回结果
        return ProcessResult(
            task_id=task.task_id,
            task_type='producer',
            pid=pid,
            start_time=start_time,
            end_time=end_time,
            success=True,
            messages_sent=messages_sent,
            bytes_sent=bytes_sent,
            errors=errors,
            latency_stats=latency_stats.__dict__
        )

    except Exception as e:
        end_time = time.time()
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"[Producer {task.task_id} PID:{pid}] ❌ 错误: {error_msg}", flush=True)

        return ProcessResult(
            task_id=task.task_id,
            task_type='producer',
            pid=pid,
            start_time=start_time,
            end_time=end_time,
            success=False,
            messages_sent=messages_sent,
            bytes_sent=bytes_sent,
            errors=errors + 1,
            error_message=error_msg
        )


async def _run_consumer_async(
    task: ConsumerTask,
    driver_config: DriverConfig
) -> ProcessResult:
    """
    异步运行 consumer 逻辑（在子进程中）
    """
    from benchmark.drivers.kafka import KafkaDriver
    from benchmark.utils.latency_recorder import EndToEndLatencyRecorder

    pid = mp.current_process().pid
    print(f"[Consumer {task.task_id} PID:{pid}] 🚀 启动", flush=True)

    start_time = time.time()
    messages_received = 0
    bytes_received = 0
    errors = 0

    try:
        # 1. 创建 Driver
        driver = KafkaDriver(driver_config)
        await driver.initialize()

        # 2. 创建 Consumer
        consumer = driver.create_consumer()

        # 3. 订阅
        await consumer.subscribe(task.topics, task.subscription_name)
        print(
            f"[Consumer {task.task_id} PID:{pid}] 📥 订阅: "
            f"topics={task.topics}, group={task.subscription_name}",
            flush=True
        )

        # 4. E2E 延迟追踪
        e2e_latency_recorder = EndToEndLatencyRecorder()

        # 5. 消费消息
        end_time = start_time + task.test_duration_seconds
        print(
            f"[Consumer {task.task_id} PID:{pid}] 🔄 开始消费 "
            f"({task.test_duration_seconds}s)...",
            flush=True
        )

        while time.time() < end_time:
            try:
                async for consumed_message in consumer.consume_messages(timeout_seconds=1.0):
                    messages_received += 1

                    # 统计字节数
                    if hasattr(consumed_message, 'message') and hasattr(consumed_message.message, 'value'):
                        message_value = consumed_message.message.value
                        if isinstance(message_value, (bytes, str)):
                            bytes_received += len(message_value)

                    # E2E 延迟
                    if hasattr(consumed_message, 'message') and hasattr(consumed_message.message, 'headers'):
                        headers = consumed_message.message.headers or {}
                        if 'send_timestamp' in headers:
                            try:
                                send_ts_bytes = headers['send_timestamp']
                                if isinstance(send_ts_bytes, bytes):
                                    send_timestamp_ms = float(send_ts_bytes.decode('utf-8'))
                                    e2e_latency_recorder.record_from_timestamp(send_timestamp_ms)
                            except:
                                pass

                    # 进度日志
                    if messages_received % 10000 == 0:
                        print(
                            f"[Consumer {task.task_id} PID:{pid}] 📊 已消费 {messages_received}",
                            flush=True
                        )

            except Exception as e:
                errors += 1
                if errors <= 10:
                    print(f"[Consumer {task.task_id} PID:{pid}] ⚠️ 消费错误: {e}", flush=True)

        # 6. 关闭
        await consumer.close()
        await driver.cleanup()

        actual_end_time = time.time()
        duration = actual_end_time - start_time

        # 7. E2E 延迟统计
        e2e_snapshot = e2e_latency_recorder.get_snapshot()

        from benchmark.utils.latency_recorder import snapshot_to_legacy_stats
        e2e_latency_stats = snapshot_to_legacy_stats(e2e_snapshot)

        print(
            f"[Consumer {task.task_id} PID:{pid}] ✅ 完成: "
            f"{messages_received} 条消息, "
            f"{messages_received/duration:.1f} msg/s, "
            f"E2E p99={e2e_snapshot.p99_ms:.2f}ms",
            flush=True
        )

        # 8. 返回结果
        return ProcessResult(
            task_id=task.task_id,
            task_type='consumer',
            pid=pid,
            start_time=start_time,
            end_time=actual_end_time,
            success=True,
            messages_received=messages_received,
            bytes_received=bytes_received,
            errors=errors,
            latency_stats=e2e_latency_stats.__dict__
        )

    except Exception as e:
        actual_end_time = time.time()
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"[Consumer {task.task_id} PID:{pid}] ❌ 错误: {error_msg}", flush=True)

        return ProcessResult(
            task_id=task.task_id,
            task_type='consumer',
            pid=pid,
            start_time=start_time,
            end_time=actual_end_time,
            success=False,
            messages_received=messages_received,
            bytes_received=bytes_received,
            errors=errors + 1,
            error_message=error_msg
        )
