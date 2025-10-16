"""
Isolated Kafka Consumer - 每个consumer在独立进程中运行，不使用Queue传递消息。
消息直接在进程内处理，统计信息定期序列化到文件，最后由主进程聚合。
"""
import logging
import multiprocessing
import time
import os
import pickle
from pathlib import Path
from confluent_kafka import Consumer
from benchmark.driver.benchmark_consumer import BenchmarkConsumer
from benchmark.worker.worker_stats import WorkerStats

logger = logging.getLogger(__name__)


def _isolated_consumer_loop(
    consumer_id: int,
    topic: str,
    properties: dict,
    stats_dir: str,
    poll_timeout: float,
    commit_interval: int,
    closing: multiprocessing.Event,
    paused: multiprocessing.Event
):
    """
    完全独立的consumer进程循环，不依赖Queue。

    :param consumer_id: Consumer ID
    :param topic: Topic name
    :param properties: Kafka consumer properties
    :param stats_dir: Directory to write stats files
    :param poll_timeout: Poll timeout in seconds
    :param commit_interval: Commit every N messages
    :param closing: Event to signal closing
    :param paused: Event to signal pause
    """
    # 设置进程级logger
    process_logger = logging.getLogger(f"Consumer-{consumer_id}")

    # 创建consumer
    consumer = Consumer(properties)
    consumer.subscribe([topic])

    # 创建本地统计对象
    stats = WorkerStats()

    message_count = 0
    was_paused = False

    # 统计文件路径
    stats_file = Path(stats_dir) / f"consumer_{consumer_id}_stats.pkl"
    last_stats_write = time.time()
    stats_write_interval = 5  # 每5秒写一次统计文件

    process_logger.info(f"Consumer {consumer_id} started, writing stats to {stats_file}")

    try:
        while not closing.is_set():
            try:
                # 处理pause/resume
                if paused.is_set():
                    if not was_paused:
                        partitions = consumer.assignment()
                        if partitions:
                            consumer.pause(partitions)
                        was_paused = True
                    time.sleep(0.1)
                    continue
                else:
                    if was_paused:
                        partitions = consumer.assignment()
                        if partitions:
                            consumer.resume(partitions)
                        was_paused = False

                # 批量poll消息（提升性能）
                messages = consumer.consume(num_messages=100, timeout=poll_timeout)

                if not messages:
                    continue

                # 处理消息
                now_ns = time.time_ns()
                for msg in messages:
                    if msg.error():
                        process_logger.error(f"Consumer error: {msg.error()}")
                        continue

                    # 计算end-to-end latency
                    timestamp_type, publish_timestamp_ms = msg.timestamp()
                    if timestamp_type != 0:  # TIMESTAMP_NOT_AVAILABLE
                        now_ms = now_ns // 1_000_000
                        e2e_latency_ms = int(now_ms - publish_timestamp_ms)
                    else:
                        e2e_latency_ms = 0

                    # 直接记录统计（无IPC开销！）
                    payload_length = len(msg.value()) if msg.value() else 0
                    stats.record_message_received(payload_length, e2e_latency_ms)

                message_count += len(messages)

                # 定期commit
                if message_count >= commit_interval:
                    try:
                        consumer.commit(asynchronous=True)
                        message_count = 0
                    except Exception as e:
                        process_logger.warning(f"Commit error: {e}")

                # 定期写入统计文件
                if time.time() - last_stats_write >= stats_write_interval:
                    _write_stats_to_file(stats, stats_file, process_logger)
                    last_stats_write = time.time()

            except Exception as e:
                process_logger.error(f"Exception in consumer loop: {e}", exc_info=True)

    finally:
        # 最后一次commit
        try:
            consumer.commit(asynchronous=False)
        except:
            pass

        # 写入最终统计
        _write_stats_to_file(stats, stats_file, process_logger)

        consumer.close()
        process_logger.info(f"Consumer {consumer_id} closed")


def _write_stats_to_file(stats: WorkerStats, stats_file: Path, logger):
    """将统计信息序列化到文件"""
    try:
        # 获取累积latency（包含完整的HdrHistogram）
        cumulative_latencies = stats.to_cumulative_latencies()
        counters = stats.to_counters_stats()

        # 序列化HdrHistogram
        data = {
            'messages_received': counters.messages_received,
            'publish_latency_encoded': cumulative_latencies.publish_latency.encode(),
            'publish_delay_latency_encoded': cumulative_latencies.publish_delay_latency.encode(),
            'end_to_end_latency_encoded': cumulative_latencies.end_to_end_latency.encode(),
            'timestamp': time.time()
        }

        # 原子写入（先写临时文件，再rename）
        temp_file = stats_file.with_suffix('.tmp')
        with open(temp_file, 'wb') as f:
            pickle.dump(data, f)
        temp_file.rename(stats_file)

    except Exception as e:
        logger.error(f"Failed to write stats to file: {e}", exc_info=True)


class KafkaBenchmarkConsumerIsolated(BenchmarkConsumer):
    """
    完全隔离的Kafka consumer实现。

    每个consumer在独立进程中运行，不使用Queue传递消息。
    消息处理和统计记录都在子进程内完成，零IPC开销。
    主进程定期/最后读取统计文件并聚合结果。
    """

    _consumer_counter = 0
    _counter_lock = multiprocessing.Lock()

    @classmethod
    def _get_next_consumer_id(cls):
        """获取下一个consumer ID（线程安全）"""
        with cls._counter_lock:
            consumer_id = cls._consumer_counter
            cls._consumer_counter += 1
            return consumer_id

    def __init__(
        self,
        topic: str,
        subscription_name: str,
        properties: dict,
        stats_dir: str = "/tmp/kafka_benchmark_stats",
        poll_timeout: float = 1.0,
        commit_interval: int = 1000
    ):
        """
        Initialize isolated Kafka benchmark consumer.

        :param topic: Topic to subscribe to
        :param subscription_name: Consumer group name
        :param properties: Consumer properties
        :param stats_dir: Directory to store stats files
        :param poll_timeout: Poll timeout in seconds (default 1.0s)
        :param commit_interval: Commit every N messages (default 1000)
        """
        # Set group.id from subscription_name
        properties['group.id'] = subscription_name

        # 优化consumer配置
        if 'fetch.min.bytes' not in properties:
            properties['fetch.min.bytes'] = 1024
        if 'fetch.max.wait.ms' not in properties:
            properties['fetch.max.wait.ms'] = 100
        if 'max.poll.records' not in properties:
            properties['max.poll.records'] = 5000

        self.consumer_id = self._get_next_consumer_id()
        self.topic = topic
        self.properties = properties
        self.poll_timeout = poll_timeout
        self.commit_interval = commit_interval
        self.stats_dir = stats_dir

        # 创建统计目录
        Path(stats_dir).mkdir(parents=True, exist_ok=True)

        self.closing = multiprocessing.Event()
        self.paused = multiprocessing.Event()

        # 启动consumer进程
        self.consumer_process = multiprocessing.Process(
            target=_isolated_consumer_loop,
            args=(
                self.consumer_id,
                self.topic,
                self.properties,
                self.stats_dir,
                self.poll_timeout,
                self.commit_interval,
                self.closing,
                self.paused
            ),
            daemon=True
        )
        self.consumer_process.start()

        logger.info(f"Started isolated consumer {self.consumer_id} (PID: {self.consumer_process.pid})")

    def get_stats_file(self) -> Path:
        """获取此consumer的统计文件路径"""
        return Path(self.stats_dir) / f"consumer_{self.consumer_id}_stats.pkl"

    def pause(self):
        """Pause consuming."""
        self.paused.set()

    def resume(self):
        """Resume consuming."""
        self.paused.clear()

    def close(self):
        """Close the consumer."""
        logger.info(f"Closing consumer {self.consumer_id}...")
        self.closing.set()

        # Wait for consumer process
        if self.consumer_process.is_alive():
            self.consumer_process.join(timeout=10)
            if self.consumer_process.is_alive():
                logger.warning(f"Consumer {self.consumer_id} did not exit cleanly, terminating...")
                self.consumer_process.terminate()
                self.consumer_process.join(timeout=2)

        logger.info(f"Consumer {self.consumer_id} closed")
