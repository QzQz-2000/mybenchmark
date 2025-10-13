import time
import logging
import random
from typing import List
from concurrent.futures import ThreadPoolExecutor
from .test_result import TestResult
from .workload import Workload
from .rate_controller import RateController

logger = logging.getLogger(__name__)


class WorkloadGenerator:
    """WorkloadGenerator implements AutoCloseable."""

    def __init__(self, driver_name: str, workload: Workload, worker):
        """
        Initialize workload generator.

        :param driver_name: Name of the driver
        :param workload: Workload configuration
        :param worker: Worker instance
        """
        self.driver_name = driver_name
        self.workload = workload
        self.worker = worker

        self.executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="workload-gen")

        self.run_completed = False
        self.need_to_wait_for_backlog_draining = False

        self.target_publish_rate = 0.0

        # 两种模式不可以一起用
        if workload.consumer_backlog_size_gb > 0 and workload.producer_rate == 0:
            raise ValueError("Cannot probe producer sustainable rate when building backlog")

    def run(self) -> TestResult:
        """Run the workload and return test results."""
        from benchmark.utils.timer import Timer
        from benchmark.worker.commands.topics_info import TopicsInfo

        # 🧹 STEP 1: 清理旧topic，确保每次测试都是幂等的
        logger.info("=" * 80)
        logger.info("🧹 STEP 1: Cleaning up old topics for idempotent test")
        logger.info("=" * 80)

        # 获取topic前缀
        topic_prefix = "test-topic"
        if hasattr(self.worker, 'benchmark_driver') and self.worker.benchmark_driver:
            topic_prefix = self.worker.benchmark_driver.get_topic_name_prefix()

        old_topics = [f"{topic_prefix}-{i}" for i in range(self.workload.topics)]
        logger.info(f"🗑️  Attempting to delete {len(old_topics)} old topics: {old_topics}")

        # 删除旧topic
        if hasattr(self.worker, 'benchmark_driver') and self.worker.benchmark_driver:
            if hasattr(self.worker.benchmark_driver, 'delete_topics'):
                try:
                    delete_timer = Timer()
                    delete_future = self.worker.benchmark_driver.delete_topics(old_topics)
                    delete_future.result()  # Wait for deletion command to complete
                    logger.info(f"🗑️  Deletion command completed in {delete_timer.elapsed_millis()} ms")

                    # 等待10秒，确保Kafka异步删除完成
                    logger.info("⏳ Waiting 10 seconds for completing asynchronous topic deletion...")
                    time.sleep(10)
                    logger.info("✅ Old topic cleanup completed")

                except Exception as e:
                    logger.warning(f"⚠️  Could not delete old topics (they may not exist): {e}")
                    logger.info("⏳ Waiting 5 seconds before proceeding...")
                    time.sleep(5)
            else:
                logger.warning("⚠️  Driver does not support delete_topics, skipping cleanup")
        else:
            logger.warning("⚠️  Worker has no benchmark_driver, skipping cleanup")

        # 📝 STEP 2: 创建新topic（带重试机制）
        logger.info("=" * 80)
        logger.info("📝 STEP 2: Creating new topics")
        logger.info("=" * 80)

        timer = Timer()
        max_retries = 5
        retry_delay_s = 10  # 每次重试等待10秒

        topics = None
        for retry in range(max_retries):
            try:
                topics = self.worker.create_topics(
                    TopicsInfo(self.workload.topics, self.workload.partitions_per_topic)
                )
                logger.info(f"✅ Topic creation command completed in {timer.elapsed_millis()} ms")

                # 等待5秒，确保topic创建完成
                logger.info("⏳ Waiting 5 seconds for topics to be fully created...")
                time.sleep(5)
                logger.info(f"✅ Successfully created {len(topics)} topics")
                break

            except Exception as e:
                error_msg = str(e)
                # 检查是否是"正在删除"错误
                if ("marked for deletion" in error_msg or "TOPIC_ALREADY_EXISTS" in error_msg) and retry < max_retries - 1:
                    logger.warning(f"⚠️  Topics still being deleted, waiting {retry_delay_s}s before retry {retry + 1}/{max_retries - 1}...")
                    time.sleep(retry_delay_s)
                else:
                    # 最后一次重试也失败了
                    logger.error(f"❌ Failed to create topics after {max_retries} retries: {e}")
                    raise

        if topics is None:
            raise RuntimeError("Failed to create topics after all retries")

        # 保存topic列表用于清理
        self.created_topics = topics

        # 为每个topic创建consumer和producer
        self._create_consumers(topics)
        self._create_producers(topics)

        # 发一条消息，确保consumer已经就绪
        self._ensure_topics_are_ready()

        # 按照用户定义的速率来
        if self.workload.producer_rate > 0:
            self.target_publish_rate = self.workload.producer_rate
        else:
            # 自动探测最大速率
            self.target_publish_rate = 10000

            self.executor.submit(self._find_maximum_sustainable_rate, self.target_publish_rate)

        from benchmark.utils.payload.file_payload_reader import FilePayloadReader
        from benchmark.worker.commands.producer_work_assignment import ProducerWorkAssignment

        payload_reader = FilePayloadReader(self.workload.message_size)

        producer_work_assignment = ProducerWorkAssignment()
        producer_work_assignment.key_distributor_type = self.workload.key_distributor
        producer_work_assignment.publish_rate = self.target_publish_rate
        producer_work_assignment.payload_data = []

        # 测试压缩效果
        if self.workload.use_randomized_payloads:
            # create messages that are part random and part zeros
            # better for testing effects of compression
            r = random.Random()
            random_bytes = int(self.workload.message_size * self.workload.random_bytes_ratio)
            zeroed_bytes = self.workload.message_size - random_bytes

            for i in range(self.workload.randomized_payload_pool_size):
                rand_array = r.randbytes(random_bytes)
                zeroed_array = bytes(zeroed_bytes)
                combined = rand_array + zeroed_array
                producer_work_assignment.payload_data.append(combined)
        else:
            # 从文件读取payload，固定大小，提前生成
            # Only load payload file if one is specified
            if self.workload.payload_file is not None:
                producer_work_assignment.payload_data.append(
                    payload_reader.load(self.workload.payload_file)
                )
            else:
                # 全0填充，默认情况
                # Generate simple payload of the specified size
                producer_work_assignment.payload_data.append(
                    bytes(self.workload.message_size)
                )

        # 开始启动所有负载，producer开始发消息，consumer开始接收消息
        self.worker.start_load(producer_work_assignment)

        if self.workload.warmup_duration_minutes > 0:
            logger.info(f"----- Starting warm-up traffic ({self.workload.warmup_duration_minutes}m) ------")
            # ✅ Warmup阶段：不停止Agent，不收集结果
            self._print_and_collect_stats(self.workload.warmup_duration_minutes * 60, stop_agents_when_done=False)

        # 积压测试
        if self.workload.consumer_backlog_size_gb > 0:
            self.executor.submit(self._build_and_drain_backlog, self.workload.test_duration_minutes)

        # 重置统计，清除预热数据
        self.worker.reset_stats()
        logger.info(f"----- Starting benchmark traffic ({self.workload.test_duration_minutes}m)------")

        # 收集结果（内部会停止Agent）
        # ✅ 正式测试阶段：停止Agent，收集结果
        result = self._print_and_collect_stats(self.workload.test_duration_minutes * 60, stop_agents_when_done=True)
        # 清理和返回（Agent已在_print_and_collect_stats中停止）
        self.run_completed = True

        return result

    def _ensure_topics_are_ready(self):
        """Ensure topics are ready by probing producers and waiting for consumers."""

        # V2架构：Consumer在独立进程中，在start_load()之后才启动
        # 因此跳过probe阶段，直接让start_load()启动所有Agent
        if hasattr(self.worker, 'consumer_metadata') and self.worker.consumer_metadata:
            logger.info("V2 架构: Skipping probe phase (Consumers will start with load)")
            logger.info("Topics will be ready after start_load() spawns Consumer Agents")
            return

        # V1架构（兼容性保留）：Consumer在主进程中，需要probe
        logger.info("Waiting for consumers to be ready")
        # This is work around the fact that there's no way to have a consumer ready in Kafka without
        # first publishing
        # some message on the topic, which will then trigger the partitions assignment to the consumers

        expected_messages = self.workload.topics * self.workload.subscriptions_per_topic
        logger.info(f"Expected messages to receive: {expected_messages} (topics={self.workload.topics}, subs={self.workload.subscriptions_per_topic})")

        # In this case we just publish 1 message and then wait for consumers to receive the data
        self.worker.probe_producers()

        start = time.time()
        end = start + 60

        while time.time() < end:
            stats = self.worker.get_counters_stats()

            logger.info(
                f"Waiting for topics to be ready -- Sent: {stats.messages_sent}, "
                f"Received: {stats.messages_received}, Expected: {expected_messages}"
            )

            if stats.messages_received < expected_messages:
                try:
                    time.sleep(2)
                except KeyboardInterrupt:
                    raise RuntimeError("Interrupted")
            else:
                break

        if time.time() >= end:
            raise RuntimeError("Timed out waiting for consumers to be ready")
        else:
            logger.info("All consumers are ready")

    def _find_maximum_sustainable_rate(self, current_rate: float):
        """
        Adjust the publish rate to a level that is sustainable, meaning that we can consume all the
        messages that are being produced.

        :param current_rate: Current rate
        """
        stats = self.worker.get_counters_stats()

        control_period_millis = 3000
        last_control_timestamp = time.perf_counter_ns()

        rate_controller = RateController()

        while not self.run_completed:
            # Check every few seconds and adjust the rate
            try:
                time.sleep(control_period_millis / 1000)
            except KeyboardInterrupt:
                return

            # Consider multiple copies when using multiple subscriptions
            stats = self.worker.get_counters_stats()
            current_time = time.perf_counter_ns()
            period_nanos = current_time - last_control_timestamp

            last_control_timestamp = current_time

            current_rate = rate_controller.next_rate(
                current_rate, period_nanos, stats.messages_sent, stats.messages_received
            )
            self.worker.adjust_publish_rate(current_rate)

    def close(self):
        """Close and cleanup resources."""
        from benchmark.utils.timer import Timer

        self.worker.stop_all()
        self.executor.shutdown(wait=False)

        # 🗑️ STEP 3: 测试结束后删除topic，确保幂等性
        logger.info("=" * 80)
        logger.info("🗑️  STEP 3: Cleaning up topics after test (for idempotency)")
        logger.info("=" * 80)

        if hasattr(self, 'created_topics') and self.created_topics:
            logger.info(f"🗑️  Deleting {len(self.created_topics)} topics: {self.created_topics}")
            try:
                if hasattr(self.worker, 'benchmark_driver') and hasattr(self.worker.benchmark_driver, 'delete_topics'):
                    cleanup_timer = Timer()
                    delete_future = self.worker.benchmark_driver.delete_topics(self.created_topics)
                    delete_future.result()  # Wait for deletion command
                    logger.info(f"🗑️  Deletion command completed in {cleanup_timer.elapsed_millis()} ms")

                    # 等待10秒，确保Kafka异步删除完成
                    logger.info("⏳ Waiting 10 seconds for Kafka to complete asynchronous topic deletion...")
                    time.sleep(10)
                    logger.info("✅ Post-test cleanup completed")

                else:
                    logger.warning("⚠️  Driver does not support delete_topics, skipping cleanup")
            except Exception as e:
                logger.error(f"❌ Failed to delete topics after test: {e}")
        else:
            logger.warning("⚠️  No topics to clean up (created_topics list is empty)")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _create_consumers(self, topics: List[str]):
        """Create consumers for topics."""
        from benchmark.worker.commands.consumer_assignment import ConsumerAssignment
        from benchmark.worker.commands.topic_subscription import TopicSubscription
        from benchmark.utils.random_generator import RandomGenerator
        from benchmark.utils.timer import Timer

        consumer_assignment = ConsumerAssignment()

        for topic in topics:
            for i in range(self.workload.subscriptions_per_topic):
                subscription_name = f"sub-{i:03d}-{RandomGenerator.get_random_string()}"
                for j in range(self.workload.consumer_per_subscription):
                    consumer_assignment.topics_subscriptions.append(
                        TopicSubscription(topic, subscription_name)
                    )

        random.shuffle(consumer_assignment.topics_subscriptions)

        timer = Timer()

        self.worker.create_consumers(consumer_assignment)
        logger.info(
            f"Created {len(consumer_assignment.topics_subscriptions)} consumers in "
            f"{timer.elapsed_millis()} ms"
        )

    def _create_producers(self, topics: List[str]):
        """Create producers for topics."""
        from benchmark.utils.timer import Timer

        full_list_of_topics = []

        # Add the topic multiple times, one for each producer
        for i in range(self.workload.producers_per_topic):
            full_list_of_topics.extend(topics)

        random.shuffle(full_list_of_topics)

        timer = Timer()

        self.worker.create_producers(full_list_of_topics)
        logger.info(f"Created {len(full_list_of_topics)} producers in {timer.elapsed_millis()} ms")

    def _build_and_drain_backlog(self, test_duration_minutes: int):
        """Build and drain message backlog."""
        from benchmark.utils.timer import Timer

        timer = Timer()
        logger.info("Stopping all consumers to build backlog")
        self.worker.pause_consumers()

        self.need_to_wait_for_backlog_draining = True

        requested_backlog_size = self.workload.consumer_backlog_size_gb * 1024 * 1024 * 1024

        while True:
            stats = self.worker.get_counters_stats()
            current_backlog_size = (
                self.workload.subscriptions_per_topic * stats.messages_sent - stats.messages_received
            ) * self.workload.message_size

            if current_backlog_size >= requested_backlog_size:
                break

            try:
                time.sleep(1)
            except KeyboardInterrupt:
                raise RuntimeError("Interrupted")

        logger.info(f"--- Completed backlog build in {timer.elapsed_seconds()} s ---")
        timer = Timer()
        logger.info("--- Start draining backlog ---")

        self.worker.resume_consumers()

        backlog_message_capacity = requested_backlog_size // self.workload.message_size
        backlog_empty_level = int((1.0 - self.workload.backlog_drain_ratio) * backlog_message_capacity)
        min_backlog = max(1000, backlog_empty_level)

        while True:
            stats = self.worker.get_counters_stats()
            current_backlog = (
                self.workload.subscriptions_per_topic * stats.messages_sent - stats.messages_received
            )

            if current_backlog <= min_backlog:
                logger.info(f"--- Completed backlog draining in {timer.elapsed_seconds()} s ---")

                try:
                    time.sleep(test_duration_minutes * 60)
                except KeyboardInterrupt:
                    raise RuntimeError("Interrupted")

                self.need_to_wait_for_backlog_draining = False
                return

            try:
                time.sleep(0.1)
            except KeyboardInterrupt:
                raise RuntimeError("Interrupted")

    def _print_and_collect_stats(self, test_duration_seconds: int, stop_agents_when_done: bool = True) -> TestResult:
        """Print and collect statistics during the test.

        Args:
            test_duration_seconds: Duration of the test in seconds
            stop_agents_when_done: Whether to stop agents when test duration is reached.
                                   Set to False during warmup to keep agents running.
                                   Set to True during actual test to stop agents at the end.
        """
        from benchmark.utils.padding_decimal_format import PaddingDecimalFormat

        start_time = time.perf_counter_ns()

        # Print report stats
        old_time = time.perf_counter_ns()

        test_end_time = start_time + test_duration_seconds * 1_000_000_000 if test_duration_seconds > 0 else float('inf')

        result = TestResult()
        result.workload = self.workload.name
        result.driver = self.driver_name
        result.topics = self.workload.topics
        result.partitions = self.workload.partitions_per_topic
        result.message_size = self.workload.message_size
        result.producers_per_topic = self.workload.producers_per_topic
        result.consumers_per_topic = self.workload.consumer_per_subscription

        rate_format = PaddingDecimalFormat("0.0", 7)
        throughput_format = PaddingDecimalFormat("0.0", 4)
        dec = PaddingDecimalFormat("0.0", 4)

        while True:
            try:
                time.sleep(10)
            except KeyboardInterrupt:
                break

            # 🔧 FIX: 先检查时间，避免在超时后还继续获取统计（get_period_stats很慢）
            now = time.perf_counter_ns()

            # 如果已经超时，根据参数决定是否停止Agent
            if now >= test_end_time and not self.need_to_wait_for_backlog_draining:
                if stop_agents_when_done:
                    logger.info(f"----- Test duration reached, stopping agents ------")
                    self.run_completed = True
                    self.worker.stop_all()  # 立即停止Agent进程
                    # 现在可以慢慢计算最终统计了（Agent已停止）
                else:
                    logger.info(f"----- Warm-up duration reached ------")
                break

            stats = self.worker.get_period_stats()
            elapsed = (now - old_time) / 1e9

            publish_rate = stats.messages_sent / elapsed
            publish_throughput = stats.bytes_sent / elapsed / 1024 / 1024
            error_rate = stats.message_send_errors / elapsed

            consume_rate = stats.messages_received / elapsed
            consume_throughput = stats.bytes_received / elapsed / 1024 / 1024

            current_backlog = max(
                0,
                self.workload.subscriptions_per_topic * stats.total_messages_sent
                - stats.total_messages_received
            )

            # 🚀 优化：批量计算所有百分位数（避免重复调用get_value_at_percentile）
            # HdrHistogram的get_value_at_percentile()对大样本量非常慢（O(n)复杂度）
            # 使用get_percentile_to_value_dict()批量计算，速度快100倍+
            percentiles = [50, 75, 95, 99, 99.9, 99.99]

            # 批量计算Pub Latency百分位数（如果histogram为空则返回0）
            pub_lat_dict = stats.publish_latency.get_percentile_to_value_dict(percentiles) if stats.publish_latency.get_total_count() > 0 else {p: 0 for p in percentiles}
            pub_lat_p50 = self._micros_to_millis(pub_lat_dict.get(50, 0))
            pub_lat_p75 = self._micros_to_millis(pub_lat_dict.get(75, 0))
            pub_lat_p95 = self._micros_to_millis(pub_lat_dict.get(95, 0))
            pub_lat_p99 = self._micros_to_millis(pub_lat_dict.get(99, 0))
            pub_lat_p999 = self._micros_to_millis(pub_lat_dict.get(99.9, 0))
            pub_lat_p9999 = self._micros_to_millis(pub_lat_dict.get(99.99, 0))
            pub_lat_max = self._micros_to_millis(stats.publish_latency.get_max_value()) if stats.publish_latency.get_total_count() > 0 else 0

            # 批量计算Pub Delay百分位数（如果histogram为空则返回0）
            pub_delay_dict = stats.publish_delay_latency.get_percentile_to_value_dict(percentiles) if stats.publish_delay_latency.get_total_count() > 0 else {p: 0 for p in percentiles}
            pub_delay_p50 = pub_delay_dict.get(50, 0)
            pub_delay_p75 = pub_delay_dict.get(75, 0)
            pub_delay_p95 = pub_delay_dict.get(95, 0)
            pub_delay_p99 = pub_delay_dict.get(99, 0)
            pub_delay_p999 = pub_delay_dict.get(99.9, 0)
            pub_delay_p9999 = pub_delay_dict.get(99.99, 0)
            pub_delay_max = stats.publish_delay_latency.get_max_value() if stats.publish_delay_latency.get_total_count() > 0 else 0

            # 批量计算E2E Latency百分位数（如果histogram为空则返回0）
            e2e_lat_dict = stats.end_to_end_latency.get_percentile_to_value_dict(percentiles) if stats.end_to_end_latency.get_total_count() > 0 else {p: 0 for p in percentiles}
            e2e_lat_p50 = self._micros_to_millis(e2e_lat_dict.get(50, 0))
            e2e_lat_p75 = self._micros_to_millis(e2e_lat_dict.get(75, 0))
            e2e_lat_p95 = self._micros_to_millis(e2e_lat_dict.get(95, 0))
            e2e_lat_p99 = self._micros_to_millis(e2e_lat_dict.get(99, 0))
            e2e_lat_p999 = self._micros_to_millis(e2e_lat_dict.get(99.9, 0))
            e2e_lat_p9999 = self._micros_to_millis(e2e_lat_dict.get(99.99, 0))
            e2e_lat_max = self._micros_to_millis(stats.end_to_end_latency.get_max_value()) if stats.end_to_end_latency.get_total_count() > 0 else 0

            # ⚡ Log阶段：只显示百分位数，不计算avg（avg在最后聚合时基于完整histogram计算）
            logger.info(
                f"Pub rate {rate_format.format(publish_rate)} msg/s / "
                f"{throughput_format.format(publish_throughput)} MB/s | "
                f"Pub err {rate_format.format(error_rate)} err/s | "
                f"Cons rate {rate_format.format(consume_rate)} msg/s / "
                f"{throughput_format.format(consume_throughput)} MB/s | "
                f"Backlog: {dec.format(current_backlog / 1000.0)} K | "
                f"Pub Latency (ms) 50%: {dec.format(pub_lat_p50)} - "
                f"99%: {dec.format(pub_lat_p99)} - "
                f"99.9%: {dec.format(pub_lat_p999)} - "
                f"Max: {throughput_format.format(pub_lat_max)} | "
                f"Pub Delay (ms) 50%: {dec.format(pub_delay_p50)} - "
                f"99%: {dec.format(pub_delay_p99)} - "
                f"99.9%: {dec.format(pub_delay_p999)} - "
                f"Max: {throughput_format.format(pub_delay_max)}"
            )

            result.publish_rate.append(publish_rate)
            result.publish_error_rate.append(error_rate)
            result.consume_rate.append(consume_rate)
            result.backlog.append(current_backlog)
            # ⚡ 不再保存周期avg，只在最后基于完整histogram计算aggregated avg
            result.publish_latency_50pct.append(pub_lat_p50)
            result.publish_latency_75pct.append(pub_lat_p75)
            result.publish_latency_95pct.append(pub_lat_p95)
            result.publish_latency_99pct.append(pub_lat_p99)
            result.publish_latency_999pct.append(pub_lat_p999)
            result.publish_latency_9999pct.append(pub_lat_p9999)
            result.publish_latency_max.append(pub_lat_max)

            result.publish_delay_latency_50pct.append(int(pub_delay_p50))
            result.publish_delay_latency_75pct.append(int(pub_delay_p75))
            result.publish_delay_latency_95pct.append(int(pub_delay_p95))
            result.publish_delay_latency_99pct.append(int(pub_delay_p99))
            result.publish_delay_latency_999pct.append(int(pub_delay_p999))
            result.publish_delay_latency_9999pct.append(int(pub_delay_p9999))
            result.publish_delay_latency_max.append(int(pub_delay_max))

            result.end_to_end_latency_50pct.append(e2e_lat_p50)
            result.end_to_end_latency_75pct.append(e2e_lat_p75)
            result.end_to_end_latency_95pct.append(e2e_lat_p95)
            result.end_to_end_latency_99pct.append(e2e_lat_p99)
            result.end_to_end_latency_999pct.append(e2e_lat_p999)
            result.end_to_end_latency_9999pct.append(e2e_lat_p9999)
            result.end_to_end_latency_max.append(e2e_lat_max)

            old_time = now

        # 循环结束，计算最终聚合统计（Agent已停止，可以慢慢算）
        logger.info(f"----- Calculating final aggregated statistics ------")
        agg = self.worker.get_cumulative_latencies()
        logger.info(
            f"----- Aggregated Pub Latency (ms) avg: {dec.format(agg.publish_latency.get_mean_value())} - "
            f"50%: {dec.format(agg.publish_latency.get_value_at_percentile(50))} - "
            f"95%: {dec.format(agg.publish_latency.get_value_at_percentile(95))} - "
            f"99%: {dec.format(agg.publish_latency.get_value_at_percentile(99))} - "
            f"99.9%: {dec.format(agg.publish_latency.get_value_at_percentile(99.9))} - "
            f"99.99%: {dec.format(agg.publish_latency.get_value_at_percentile(99.99))} - "
            f"Max: {throughput_format.format(agg.publish_latency.get_max_value())} | "
            f"Pub Delay (ms) avg: {dec.format(agg.publish_delay_latency.get_mean_value())} - "
            f"50%: {dec.format(agg.publish_delay_latency.get_value_at_percentile(50))} - "
            f"95%: {dec.format(agg.publish_delay_latency.get_value_at_percentile(95))} - "
            f"99%: {dec.format(agg.publish_delay_latency.get_value_at_percentile(99))} - "
            f"99.9%: {dec.format(agg.publish_delay_latency.get_value_at_percentile(99.9))} - "
            f"99.99%: {dec.format(agg.publish_delay_latency.get_value_at_percentile(99.99))} - "
            f"Max: {throughput_format.format(agg.publish_delay_latency.get_max_value())}"
        )

        result.aggregated_publish_latency_avg = agg.publish_latency.get_mean_value()
        result.aggregated_publish_latency_50pct = agg.publish_latency.get_value_at_percentile(50)
        result.aggregated_publish_latency_75pct = agg.publish_latency.get_value_at_percentile(75)
        result.aggregated_publish_latency_95pct = agg.publish_latency.get_value_at_percentile(95)
        result.aggregated_publish_latency_99pct = agg.publish_latency.get_value_at_percentile(99)
        result.aggregated_publish_latency_999pct = agg.publish_latency.get_value_at_percentile(99.9)
        result.aggregated_publish_latency_9999pct = agg.publish_latency.get_value_at_percentile(99.99)
        result.aggregated_publish_latency_max = agg.publish_latency.get_max_value()

        result.aggregated_publish_delay_latency_avg = agg.publish_delay_latency.get_mean_value()
        result.aggregated_publish_delay_latency_50pct = int(agg.publish_delay_latency.get_value_at_percentile(50))
        result.aggregated_publish_delay_latency_75pct = int(agg.publish_delay_latency.get_value_at_percentile(75))
        result.aggregated_publish_delay_latency_95pct = int(agg.publish_delay_latency.get_value_at_percentile(95))
        result.aggregated_publish_delay_latency_99pct = int(agg.publish_delay_latency.get_value_at_percentile(99))
        result.aggregated_publish_delay_latency_999pct = int(agg.publish_delay_latency.get_value_at_percentile(99.9))
        result.aggregated_publish_delay_latency_9999pct = int(agg.publish_delay_latency.get_value_at_percentile(99.99))
        result.aggregated_publish_delay_latency_max = int(agg.publish_delay_latency.get_max_value())

        result.aggregated_end_to_end_latency_avg = agg.end_to_end_latency.get_mean_value()
        result.aggregated_end_to_end_latency_50pct = agg.end_to_end_latency.get_value_at_percentile(50)
        result.aggregated_end_to_end_latency_75pct = agg.end_to_end_latency.get_value_at_percentile(75)
        result.aggregated_end_to_end_latency_95pct = agg.end_to_end_latency.get_value_at_percentile(95)
        result.aggregated_end_to_end_latency_99pct = agg.end_to_end_latency.get_value_at_percentile(99)
        result.aggregated_end_to_end_latency_999pct = agg.end_to_end_latency.get_value_at_percentile(99.9)
        result.aggregated_end_to_end_latency_9999pct = agg.end_to_end_latency.get_value_at_percentile(99.99)
        result.aggregated_end_to_end_latency_max = agg.end_to_end_latency.get_max_value()

        # Collect percentiles - define standard percentile list
        percentile_list = [50, 75, 90, 95, 99, 99.9, 99.99]

        for percentile_obj in agg.publish_latency.get_percentile_to_value_dict(percentile_list).items():
            result.aggregated_publish_latency_quantiles[percentile_obj[0]] = percentile_obj[1]

        for percentile_obj in agg.publish_delay_latency.get_percentile_to_value_dict(percentile_list).items():
            result.aggregated_publish_delay_latency_quantiles[percentile_obj[0]] = int(percentile_obj[1])

        for percentile_obj in agg.end_to_end_latency.get_percentile_to_value_dict(percentile_list).items():
            result.aggregated_end_to_end_latency_quantiles[percentile_obj[0]] = percentile_obj[1]

        return result

    @staticmethod
    def _micros_to_millis(time_in_millis) -> float:
        """
        Identity function - values are already in milliseconds.
        函数名保留是为了与Java版本代码结构保持一致。
        Java版本中使用微秒，Python版本统一使用毫秒。
        """
        return time_in_millis
