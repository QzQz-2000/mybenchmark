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
    多进程执行器 - Java OMB style with stop signal

    负责管理多个独立的 producer/consumer 进程，每个任务运行在独立进程中。
    使用共享的 stop_event 来控制所有进程的停止（类似 Java OMB 的 testCompleted flag）。
    """

    def __init__(self, worker_id: str):
        super().__init__()
        self.worker_id = worker_id
        self._processes: List[mp.Process] = []
        self._result_queue = mp.Queue()
        self._stop_event = mp.Event()  # Shared stop signal (Java OMB style)

    def reset(self):
        """Reset executor state for a new test run."""
        self._processes.clear()
        self._stop_event.clear()
        # Clear result queue
        while not self._result_queue.empty():
            try:
                self._result_queue.get_nowait()
            except:
                break
        self.logger.info("ProcessExecutor reset for new test run")

    async def execute_producer_tasks(
        self,
        tasks: List[ProducerTask],
        driver_config: DriverConfig
    ) -> None:
        """
        启动多个 producer 任务，每个任务一个独立进程 (Java OMB style)

        进程将持续运行直到收到 stop 信号。
        此方法启动进程后立即返回，不等待完成。

        Args:
            tasks: Producer 任务列表
            driver_config: Driver 配置
        """
        # NOTE: Do NOT clear _processes or _stop_event here!
        # In Java OMB style, consumers are started first, then producers.
        # Both share the same process list and stop signal.

        self.logger.info(f"🚀 启动 {len(tasks)} 个独立 Producer 进程 (持续模式)...")

        # 启动所有 producer 进程 - 传递 stop_event
        for task in tasks:
            process = mp.Process(
                target=_producer_process_main,
                args=(task, driver_config, self._result_queue, self._stop_event),  # Pass stop_event
                name=f"producer-{task.task_id}"
            )
            process.start()
            self._processes.append(process)

            self.logger.info(
                f"   ✅ 启动 Producer: {task.task_id} (PID: {process.pid})"
            )

        self.logger.info(f"✨ 所有 {len(tasks)} 个 Producer 进程已启动，等待 stop 信号...")

    async def execute_consumer_tasks(
        self,
        tasks: List[ConsumerTask],
        driver_config: DriverConfig
    ) -> None:
        """
        启动多个 consumer 任务，每个任务一个独立进程 (Java OMB style)

        进程将持续运行直到收到 stop 信号。
        此方法启动进程后立即返回，不等待完成。

        在 Java OMB 风格中，Consumers 先启动，所以在这里重置状态。

        Args:
            tasks: Consumer 任务列表
            driver_config: Driver 配置
        """
        # Reset state for new test run (consumers are started first)
        self.reset()

        self.logger.info(f"🚀 启动 {len(tasks)} 个独立 Consumer 进程 (持续模式)...")

        # 启动所有 consumer 进程 - 传递 stop_event
        for task in tasks:
            process = mp.Process(
                target=_consumer_process_main,
                args=(task, driver_config, self._result_queue, self._stop_event),  # Pass stop_event
                name=f"consumer-{task.task_id}"
            )
            process.start()
            self._processes.append(process)

            self.logger.info(
                f"   ✅ 启动 Consumer: {task.task_id} (PID: {process.pid})"
            )

        self.logger.info(f"✨ 所有 {len(tasks)} 个 Consumer 进程已启动，等待 stop 信号...")

    async def stop_all(self):
        """
        Stop all running processes - Java OMB style

        Sets the stop_event to signal all processes to complete gracefully.
        """
        self.logger.info("🛑 Setting stop signal for all processes...")
        self._stop_event.set()

    async def wait_for_completion(self) -> List[ProcessResult]:
        """
        Wait for all processes to complete and collect results.

        This should be called after stop_all() has been called.
        """
        total_processes = len(self._processes)
        self.logger.info(f"⏳ Waiting for {total_processes} processes to complete...")

        results = []

        # Wait for all processes to finish
        for process in self._processes:
            process.join()

        # Collect all results
        while not self._result_queue.empty():
            try:
                result_dict = self._result_queue.get_nowait()
                result = ProcessResult(**result_dict)
                results.append(result)
            except Exception as e:
                self.logger.error(f"收集结果失败: {e}")

        if len(results) != total_processes:
            self.logger.warning(
                f"期望 {total_processes} 个结果，实际收到 {len(results)} 个"
            )

        self.logger.info(f"✅ 所有 {total_processes} 个进程已完成，收到 {len(results)} 个结果")

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
    result_queue: mp.Queue,
    stop_event: mp.Event  # NEW: stop signal from coordinator
):
    """
    Producer 进程主函数（运行在独立进程中）- Java OMB style

    Runs continuously until stop_event is set by coordinator.
    模拟真实的独立应用，持续发送消息直到收到停止信号。
    """
    try:
        # 在子进程中需要重新导入和初始化
        import asyncio

        # 运行异步逻辑（传递 stop_event）
        result = asyncio.run(_run_producer_async(task, driver_config, stop_event))

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
    result_queue: mp.Queue,
    stop_event: mp.Event  # NEW: stop signal from coordinator
):
    """
    Consumer 进程主函数（运行在独立进程中）- Java OMB style

    Runs continuously until stop_event is set by coordinator.
    模拟真实的独立应用，持续消费消息直到收到停止信号。
    """
    try:
        # 在子进程中需要重新导入和初始化
        import asyncio

        # 运行异步逻辑（传递 stop_event）
        result = asyncio.run(_run_consumer_async(task, driver_config, stop_event))

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
    driver_config: DriverConfig,
    stop_event: mp.Event
) -> ProcessResult:
    """
    异步运行 producer 逻辑（在子进程中）- Java OMB continuous mode

    Runs continuously at specified rate until stop_event is set.
    """
    from benchmark.drivers.kafka import KafkaDriver
    from benchmark.drivers.base import Message, DriverUtils
    from benchmark.utils.rate_limiter import create_rate_limiter

    pid = mp.current_process().pid
    print(f"[Producer {task.task_id} PID:{pid}] 🚀 启动 (持续模式)", flush=True)

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
            print(f"[Producer {task.task_id} PID:{pid}] 📊 速率限制: {task.rate_limit} msg/s", flush=True)

        # 4. 生成 payload
        payload = DriverUtils.create_test_payload(
            task.message_size,
            task.payload_data
        )

        # 5. 持续发送消息直到收到停止信号 (Java OMB style)
        print(f"[Producer {task.task_id} PID:{pid}] 📤 开始持续发送消息...", flush=True)

        message_counter = 0
        while not stop_event.is_set():
            message_counter += 1

            # 速率限制
            if rate_limiter:
                await rate_limiter.acquire()

            # 生成消息 - 使用 counter 而不是固定的 num_messages
            key = DriverUtils.generate_message_key(
                task.key_pattern, message_counter, 0  # 0 means unlimited
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

            # 进度日志 (每10000条打印一次)
            if messages_sent > 0 and messages_sent % 10000 == 0:
                elapsed = time.time() - start_time
                rate = messages_sent / elapsed if elapsed > 0 else 0
                print(f"[Producer {task.task_id} PID:{pid}] 📊 已发送 {messages_sent} 条 ({rate:.1f} msg/s)", flush=True)

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
    driver_config: DriverConfig,
    stop_event: mp.Event
) -> ProcessResult:
    """
    异步运行 consumer 逻辑（在子进程中）- Java OMB continuous mode

    Runs continuously until stop_event is set by coordinator.
    """
    from benchmark.drivers.kafka import KafkaDriver
    from benchmark.utils.latency_recorder import EndToEndLatencyRecorder

    pid = mp.current_process().pid
    print(f"[Consumer {task.task_id} PID:{pid}] 🚀 启动 (持续模式)", flush=True)

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

        # 5. 持续消费消息直到收到停止信号 (Java OMB style)
        print(
            f"[Consumer {task.task_id} PID:{pid}] 🔄 开始持续消费消息...",
            flush=True
        )

        while not stop_event.is_set():
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

                    # 进度日志 (每10000条打印一次)
                    if messages_received % 10000 == 0:
                        elapsed = time.time() - start_time
                        rate = messages_received / elapsed if elapsed > 0 else 0
                        print(
                            f"[Consumer {task.task_id} PID:{pid}] 📊 已消费 {messages_received} 条 ({rate:.1f} msg/s)",
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
