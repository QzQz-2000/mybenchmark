# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
LocalWorker - 多进程ISOLATED架构
每个Producer作为独立进程运行，模拟真实的数字孪生Agent

这是唯一的LocalWorker实现（旧的SHARED多线程模式已废弃）
"""

import logging
import multiprocessing
import threading
import time
import importlib
from typing import List
from .worker import Worker
from .worker_stats import WorkerStats
from .isolated_agent_worker import isolated_agent_worker
from .isolated_consumer_agent import isolated_consumer_agent
from .commands.consumer_assignment import ConsumerAssignment
from .commands.counters_stats import CountersStats
from .commands.cumulative_latencies import CumulativeLatencies
from .commands.period_stats import PeriodStats
from .commands.producer_work_assignment import ProducerWorkAssignment
from .commands.topics_info import TopicsInfo

logger = logging.getLogger(__name__)


class LocalWorker(Worker):
    """
    LocalWorker - 多进程ISOLATED架构

    每个Agent作为独立进程运行，完全隔离
    适用于数字孪生场景：IoT设备、自动驾驶车辆、智能制造设备等

    特性:
    - 真实独立: 每个Agent独立进程，完全隔离
    - 真实连接: 每个Agent独立Kafka连接
    - 无GIL限制: 真正并行执行
    - 动态速率: 支持运行时速率调整
    - 统计聚合: 自动收集所有Agent统计
    """

    def __init__(self, stats_logger=None):
        """
        Initialize local worker in ISOLATED mode.

        :param stats_logger: Optional stats logger
        """
        self.benchmark_driver = None
        self.producers = []  # Producer元数据（不是实际对象，Agent进程内创建）
        self.consumers = []  # V1兼容性保留（将来可以删除）
        self.consumer_metadata = []  # Consumer元数据（V2架构：每个Consumer独立进程）
        self.executor = None
        self.stats = WorkerStats(stats_logger)
        self.test_completed = multiprocessing.Event()
        self.consumers_are_paused = False
        self._lock = threading.Lock()

        # ISOLATED模式核心组件
        self.agent_processes = []  # Agent进程列表
        self.stop_agents = multiprocessing.Event()  # Agent停止信号
        # 队列容量设置（考虑macOS系统限制：信号量上限32767）
        # 设置为32000以保持跨平台兼容性
        # 容量计算：100个Agent * 每秒1次 * 320秒缓冲 = 32000
        self.stats_queue = multiprocessing.Queue(maxsize=32000)  # 跨进程统计队列
        self.shared_publish_rate = multiprocessing.Value('d', 1.0)  # 共享速率（支持动态调整）
        self.reset_stats_flag = multiprocessing.Value('i', 0)  # 重置统计标志（epoch计数器）
        self.agent_ready_queue = multiprocessing.Queue(maxsize=32000)  # Agent就绪/错误信号队列

        # 统计收集线程
        self.stats_collector_thread = None
        self.stats_collector_running = False

        logger.info("LocalWorker initialized (multi-process ISOLATED architecture)")

    def initialize_driver(self, configuration_file: str):
        """Initialize the benchmark driver."""
        import yaml

        if self.benchmark_driver is not None:
            raise RuntimeError("Driver already initialized")

        self.test_completed.clear()

        # Load driver configuration
        with open(configuration_file, 'r') as f:
            driver_config = yaml.safe_load(f)

        logger.info(f"Driver config: {driver_config}")

        try:
            # Dynamically load driver class
            driver_class_name = driver_config['driverClass']
            module_name, class_name = driver_class_name.rsplit('.', 1)
            module = importlib.import_module(module_name)
            driver_class = getattr(module, class_name)

            # Instantiate driver
            self.benchmark_driver = driver_class()
            self.benchmark_driver.initialize(configuration_file, self.stats.get_stats_logger())

        except Exception as e:
            raise RuntimeError(f"Failed to initialize driver: {e}") from e

    def create_topics(self, topics_info: TopicsInfo) -> List[str]:
        """Create topics."""
        if self.benchmark_driver is None:
            raise RuntimeError("Driver not initialized")

        topic_name_prefix = self.benchmark_driver.get_topic_name_prefix()
        topics = []

        for i in range(topics_info.number_of_topics):
            topic_name = f"{topic_name_prefix}-{i}"
            topics.append(topic_name)

        # Create topics using driver
        topic_infos = [
            {'topic': topic, 'partitions': topics_info.number_of_partitions_per_topic}
            for topic in topics
        ]

        # Wait for all topics to be created
        future = self.benchmark_driver.create_topics(topic_infos)
        future.result()  # Block until creation completes, raise exception if failed

        return topics

    def create_producers(self, topics: List[str]):
        """
        Create producer metadata (不创建实际对象).
        在ISOLATED模式下，每个Agent进程会在自己内部创建Producer.
        """
        if self.benchmark_driver is None:
            raise RuntimeError("Driver not initialized")

        # 只保存元数据
        class ProducerMeta:
            def __init__(self, id, topic):
                self.id = id
                self.topic = topic

        self.producers = [
            ProducerMeta(i, topic)
            for i, topic in enumerate(topics)
        ]

        logger.info(f"Registered {len(self.producers)} producer metadata (Agents will create actual producers)")

    def create_consumers(self, consumer_assignment: ConsumerAssignment):
        """
        Create consumer metadata (V2架构：不创建实际对象).
        在V2架构下，每个Consumer作为独立进程运行，在进程内部创建Kafka Consumer.
        """
        if self.benchmark_driver is None:
            raise RuntimeError("Driver not initialized")

        # V2: 只保存Consumer元数据
        class ConsumerMeta:
            def __init__(self, id, topic, subscription):
                self.id = id
                self.topic = topic
                self.subscription = subscription

        self.consumer_metadata = [
            ConsumerMeta(i, ts.topic, ts.subscription)
            for i, ts in enumerate(consumer_assignment.topics_subscriptions)
        ]

        logger.info(f"Registered {len(self.consumer_metadata)} consumer metadata (V2: Agents will create actual consumers)")

        # V1兼容性: 如果需要V1模式（所有Consumer在主进程），取消注释下面代码
        # class ConsumerInfo:
        #     def __init__(self, id, topic, subscription, callback):
        #         self.id = id
        #         self.topic = topic
        #         self.subscription_name = subscription
        #         self.consumer_callback = callback
        #
        # consumer_infos = [
        #     ConsumerInfo(i, ts.topic, ts.subscription, self)
        #     for i, ts in enumerate(consumer_assignment.topics_subscriptions)
        # ]
        #
        # consumers_future = self.benchmark_driver.create_consumers(consumer_infos)
        # self.consumers = consumers_future.result()
        # logger.info(f"Created {len(self.consumers)} consumers (V1 mode)")

    def probe_producers(self):
        """
        Probe producers by sending one test message per topic.
        在ISOLATED模式下，我们直接使用driver发送测试消息到每个topic.
        """
        import time
        logger.info("Probing topics with test messages")

        if not self.producers:
            return

        # Get unique topics from all producers
        unique_topics = list(set(p.topic for p in self.producers))
        logger.info(f"Sending probe message to {len(unique_topics)} topics: {unique_topics}")

        # Send one probe message to each topic
        test_producers = []
        for topic in unique_topics:
            test_producer_future = self.benchmark_driver.create_producer(topic)
            test_producer = test_producer_future.result()
            test_producers.append(test_producer)

            # Record the send in stats
            self.stats.record_message_sent()

            # Send probe message
            test_producer.send_async(None, b"probe")
            logger.info(f"Sent probe message to topic: {topic}")

        # Wait for messages to be delivered
        time.sleep(1.0)

        # Close all test producers
        for test_producer in test_producers:
            test_producer.close()

        time.sleep(1.0)
        logger.info(f"Probe complete: sent {len(unique_topics)} messages to {len(unique_topics)} topics")

    def start_load(self, producer_work_assignment: ProducerWorkAssignment):
        """
        启动负载生成 - V2 ISOLATED模式
        为每个Producer创建一个独立的Agent进程
        为每个Consumer创建一个独立的Agent进程
        """
        if not self.producers and not self.consumer_metadata:
            logger.error("No producers or consumers registered, cannot start load")
            return

        publish_rate = producer_work_assignment.publish_rate
        num_producer_agents = len(self.producers)
        num_consumer_agents = len(self.consumer_metadata)
        total_agents = num_producer_agents + num_consumer_agents

        logger.info(f"=" * 80)
        logger.info(f"Starting V2 ISOLATED mode: {total_agents} independent Agent processes")
        logger.info(f"  - Producer Agents: {num_producer_agents} (each @ {publish_rate} msg/s)")
        logger.info(f"  - Consumer Agents: {num_consumer_agents}")
        logger.info(f"Total publish throughput: {num_producer_agents * publish_rate} msg/s")
        logger.info(f"=" * 80)

        # 设置共享速率
        self.shared_publish_rate.value = publish_rate
        self.stop_agents.clear()

        # 检测驱动类型并获取配置
        # Pulsar驱动有get_client_properties()方法，Kafka驱动没有
        is_pulsar = hasattr(self.benchmark_driver, 'get_client_properties')

        if is_pulsar:
            # Pulsar配置
            logger.info("Detected Pulsar driver, using Pulsar agent workers")
            from .isolated_pulsar_agent_worker import isolated_pulsar_agent_worker
            from .isolated_pulsar_consumer_agent import isolated_pulsar_consumer_agent

            pulsar_client_config = self.benchmark_driver.get_client_properties()
            pulsar_producer_config = self.benchmark_driver.get_producer_properties()
            pulsar_consumer_config = self.benchmark_driver.get_consumer_properties()

            producer_worker_func = isolated_pulsar_agent_worker
            consumer_worker_func = isolated_pulsar_consumer_agent
        else:
            # Kafka配置（默认）
            logger.info("Detected Kafka driver, using Kafka agent workers")
            kafka_producer_config = self.benchmark_driver.get_producer_properties()
            kafka_consumer_config = self.benchmark_driver.get_consumer_properties()

            producer_worker_func = isolated_agent_worker
            consumer_worker_func = isolated_consumer_agent

        # 启动统计收集线程
        self._start_stats_collector()

        # 1. 为每个Producer创建独立的Agent进程
        for i, producer_meta in enumerate(self.producers):
            if is_pulsar:
                # Pulsar agent arguments
                process = multiprocessing.Process(
                    target=producer_worker_func,
                    args=(
                        i,                              # agent_id
                        producer_meta.topic,            # topic
                        pulsar_client_config,           # Pulsar client config
                        pulsar_producer_config,         # Pulsar producer config
                        producer_work_assignment,       # work assignment
                        self.stop_agents,               # stop event
                        self.stats_queue,               # stats queue
                        self.shared_publish_rate,       # shared rate (for dynamic adjustment)
                        self.reset_stats_flag,          # reset stats flag
                        self.agent_ready_queue          # ready/error queue
                    ),
                    name=f"pulsar-producer-agent-{i}",
                    daemon=False
                )
            else:
                # Kafka agent arguments
                process = multiprocessing.Process(
                    target=producer_worker_func,
                    args=(
                        i,                              # agent_id
                        producer_meta.topic,            # topic
                        kafka_producer_config,          # Kafka producer config
                        kafka_consumer_config,          # Kafka consumer config
                        producer_work_assignment,       # work assignment
                        self.stop_agents,               # stop event
                        self.stats_queue,               # stats queue
                        self.shared_publish_rate,       # shared rate (for dynamic adjustment)
                        self.reset_stats_flag,          # reset stats flag
                        self.agent_ready_queue          # ready/error queue
                    ),
                    name=f"kafka-producer-agent-{i}",
                    daemon=False  # 非daemon，确保正常关闭
                )

            process.start()
            self.agent_processes.append(process)

        logger.info(f"Started {num_producer_agents} Producer Agent processes")

        # 2. 为每个Consumer创建独立的Agent进程 (V2新增)
        for i, consumer_meta in enumerate(self.consumer_metadata):
            if is_pulsar:
                # Pulsar consumer agent arguments
                process = multiprocessing.Process(
                    target=consumer_worker_func,
                    args=(
                        i,                              # agent_id
                        consumer_meta.topic,            # topic
                        consumer_meta.subscription,     # subscription name
                        pulsar_client_config,           # Pulsar client config
                        pulsar_consumer_config,         # Pulsar consumer config
                        self.stop_agents,               # stop event
                        self.stats_queue,               # stats queue
                        self.reset_stats_flag,          # reset stats flag
                        self.agent_ready_queue          # ready/error queue
                    ),
                    name=f"pulsar-consumer-agent-{i}",
                    daemon=False
                )
            else:
                # Kafka consumer agent arguments
                process = multiprocessing.Process(
                    target=consumer_worker_func,
                    args=(
                        i,                              # agent_id
                        consumer_meta.topic,            # topic
                        consumer_meta.subscription,     # subscription name
                        kafka_consumer_config,          # Kafka consumer config
                        self.stop_agents,               # stop event
                        self.stats_queue,               # stats queue
                        self.reset_stats_flag,          # reset stats flag
                        self.agent_ready_queue          # ready/error queue
                    ),
                    name=f"kafka-consumer-agent-{i}",
                    daemon=False
                )

            process.start()
            self.agent_processes.append(process)

        logger.info(f"Started {num_consumer_agents} Consumer Agent processes")
        logger.info(f"Total: {len(self.agent_processes)} Agent processes running")

        # 等待所有Agent发送就绪信号（或错误）
        ready_count = 0
        errors = []
        # 根据Agent数量动态调整超时时间: 基础10秒 + 每个Agent 0.5秒
        timeout_total = 10.0 + (total_agents * 0.5)
        deadline = time.time() + timeout_total
        logger.info(f"Waiting for {total_agents} Agents to report ready (timeout: {timeout_total:.1f}s)")

        for i in range(total_agents):
            remaining = max(0.1, deadline - time.time())
            try:
                msg = self.agent_ready_queue.get(timeout=remaining)
                agent_id = msg.get('agent_id')
                agent_type = msg.get('type', 'unknown')
                status = msg.get('status')

                if status == 'ready':
                    ready_count += 1
                    logger.info(f"{agent_type.capitalize()} Agent {agent_id} is ready ({ready_count}/{total_agents})")
                elif status == 'error':
                    error_msg = msg.get('error', 'Unknown error')
                    errors.append(f"{agent_type.capitalize()} Agent {agent_id}: {error_msg}")
                    logger.error(f"{agent_type.capitalize()} Agent {agent_id} failed to start: {error_msg}")
            except:
                # 超时，检查进程状态
                break

        # 最终健康检查
        alive_count = sum(1 for p in self.agent_processes if p.is_alive())
        logger.info(f"Agent startup complete: {ready_count} ready, {alive_count} alive, {len(errors)} errors")

        if errors:
            error_summary = "; ".join(errors[:5])  # 只显示前5个错误
            raise RuntimeError(f"Failed to start {len(errors)} Agent(s): {error_summary}")

        if ready_count < total_agents:
            logger.warning(f"Warning: Only {ready_count}/{total_agents} Agents reported ready (timeout or crash)")

    def _start_stats_collector(self):
        """启动统计收集线程（从Agent进程收集统计）"""
        self.stats_collector_running = True

        def collector_loop():
            """统计收集线程主循环"""
            from hdrh.histogram import HdrHistogram

            logger.info("Stats collector thread started")

            while self.stats_collector_running:
                try:
                    # 非阻塞获取统计数据（timeout=0.5秒）
                    try:
                        stats_dict = self.stats_queue.get(timeout=0.5)
                    except:
                        continue

                    agent_id = stats_dict.get('agent_id')

                    # 检查是否是最终统计
                    if stats_dict.get('final'):
                        logger.debug(f"Agent {agent_id} sent final stats: {stats_dict.get('total_messages')} total messages")
                        continue

                    # 检查epoch，丢弃旧epoch的数据
                    stats_epoch = stats_dict.get('epoch', 0)
                    current_epoch = self.reset_stats_flag.value
                    if stats_epoch < current_epoch:
                        logger.debug(f"Dropping stats from Agent {agent_id}: old epoch {stats_epoch} < current {current_epoch}")
                        continue

                    # 区分Producer和Consumer统计
                    agent_type = stats_dict.get('type', 'producer')

                    if agent_type == 'producer':
                        # Producer统计
                        messages_sent = stats_dict.get('messages_sent', 0)
                        bytes_sent = stats_dict.get('bytes_sent', 0)
                        errors = stats_dict.get('errors', 0)

                        # 更新主统计对象（原子操作）
                        if messages_sent > 0:
                            self.stats.messages_sent.add(messages_sent)
                            self.stats.total_messages_sent.add(messages_sent)
                        if bytes_sent > 0:
                            self.stats.bytes_sent.add(bytes_sent)
                            self.stats.total_bytes_sent.add(bytes_sent)
                        if errors > 0:
                            self.stats.message_send_errors.add(errors)
                            self.stats.total_message_send_errors.add(errors)

                    elif agent_type == 'consumer':
                        # Consumer统计 (V2新增)
                        messages_received = stats_dict.get('messages_received', 0)
                        bytes_received = stats_dict.get('bytes_received', 0)

                        # 更新主统计对象
                        if messages_received > 0:
                            self.stats.messages_received.add(messages_received)
                            self.stats.total_messages_received.add(messages_received)
                        if bytes_received > 0:
                            self.stats.bytes_received.add(bytes_received)
                            self.stats.total_bytes_received.add(bytes_received)

                    # 处理延迟统计：合并Agent的histogram到主histogram（与Java版本一致）
                    # Java版本：每个worker有自己的Recorder，定期合并
                    # Python版本：从Agent进程收集编码后的histogram，解码并合并到Recorder和累积histogram
                    # Recorder用于周期统计(get_interval_histogram)，cumulative用于累积统计

                    # Producer: 发布延迟
                    if agent_type == 'producer':
                        pub_latency_encoded = stats_dict.get('pub_latency_histogram_encoded')
                        pub_delay_encoded = stats_dict.get('pub_delay_histogram_encoded')

                        if pub_latency_encoded:
                            try:
                                # 解码histogram
                                agent_pub_latency_hist = HdrHistogram.decode(pub_latency_encoded)

                                # 高效合并到Recorder（用于周期统计）- O(n)复杂度，n为bucket数量
                                self.stats.publish_latency_recorder.record_histogram(agent_pub_latency_hist)

                                # 合并到累积直方图（用于累积统计）
                                with self.stats.histogram_lock:
                                    self.stats.cumulative_publish_latency.add(agent_pub_latency_hist)

                                logger.debug(f"📊 合并Agent {agent_id}的pub latency histogram (count={agent_pub_latency_hist.get_total_count()})")
                            except Exception as e:
                                logger.warning(f"Failed to decode/merge publish latency histogram from Agent {agent_id}: {e}")

                        if pub_delay_encoded:
                            try:
                                agent_pub_delay_hist = HdrHistogram.decode(pub_delay_encoded)

                                # 高效合并到Recorder（用于周期统计）
                                self.stats.publish_delay_latency_recorder.record_histogram(agent_pub_delay_hist)

                                # 合并到累积直方图（用于累积统计）
                                with self.stats.histogram_lock:
                                    self.stats.cumulative_publish_delay_latency.add(agent_pub_delay_hist)

                                logger.debug(f"📊 合并Agent {agent_id}的pub delay histogram (count={agent_pub_delay_hist.get_total_count()})")
                            except Exception as e:
                                logger.warning(f"Failed to decode/merge publish delay histogram from Agent {agent_id}: {e}")

                    # Consumer: 端到端延迟 (V2新增)
                    if agent_type == 'consumer':
                        e2e_latency_encoded = stats_dict.get('e2e_latency_histogram_encoded')

                        if e2e_latency_encoded:
                            try:
                                # 解码histogram
                                agent_e2e_hist = HdrHistogram.decode(e2e_latency_encoded)

                                # 高效合并到Recorder（用于周期统计）
                                self.stats.end_to_end_latency_recorder.record_histogram(agent_e2e_hist)

                                # 合并到累积直方图（用于累积统计）
                                with self.stats.histogram_lock:
                                    self.stats.cumulative_end_to_end_latency.add(agent_e2e_hist)

                                logger.debug(f"📊 合并Consumer Agent {agent_id}的e2e histogram (count={agent_e2e_hist.get_total_count()})")
                            except Exception as e:
                                logger.warning(f"Failed to decode/merge e2e latency histogram from Consumer Agent {agent_id}: {e}")

                except Exception as e:
                    logger.error(f"Error in stats collector: {e}", exc_info=True)

            logger.info("Stats collector thread stopped")

        self.stats_collector_thread = threading.Thread(
            target=collector_loop,
            name="stats-collector",
            daemon=True
        )
        self.stats_collector_thread.start()

    def adjust_publish_rate(self, publish_rate: float):
        """
        动态调整发布速率 - ISOLATED模式
        更新共享变量，所有Agent进程会定期检查并更新自己的速率
        """
        self.shared_publish_rate.value = publish_rate
        logger.info(f"Adjusted publish rate to: {publish_rate} msg/s per Agent (total: {publish_rate * len(self.agent_processes)} msg/s)")

    def pause_consumers(self):
        """
        Pause all consumers.
        V2架构注意：Consumer在独立进程中，暂停功能当前未实现
        如需实现，需要通过multiprocessing.Event通知Consumer Agent暂停poll
        """
        with self._lock:
            self.consumers_are_paused = True
            # V1兼容代码（当consumer_metadata为空且consumers有值时才执行）
            if not self.consumer_metadata and self.consumers:
                for consumer in self.consumers:
                    consumer.pause()
            elif self.consumer_metadata:
                logger.warning("V2 架构: pause_consumers() 暂未实现（Consumer在独立进程）")

    def resume_consumers(self):
        """
        Resume all consumers.
        V2架构注意：Consumer在独立进程中，恢复功能当前未实现
        """
        with self._lock:
            self.consumers_are_paused = False
            # V1兼容代码
            if not self.consumer_metadata and self.consumers:
                for consumer in self.consumers:
                    consumer.resume()
            elif self.consumer_metadata:
                logger.warning("V2 架构: resume_consumers() 暂未实现（Consumer在独立进程）")

    def get_counters_stats(self) -> CountersStats:
        """Get counter statistics."""
        return self.stats.to_counters_stats()

    def get_period_stats(self) -> PeriodStats:
        """Get period statistics."""
        return self.stats.to_period_stats()

    def get_cumulative_latencies(self) -> CumulativeLatencies:
        """Get cumulative latencies."""
        return self.stats.to_cumulative_latencies()

    def reset_stats(self):
        """
        Reset all statistics - 使用epoch机制避免竞态条件.

        策略:
        1. 递增reset_stats_flag（新的epoch）
        2. Agent进程看到新epoch后，重置本地统计并在下次汇报时带上新epoch
        3. 统计收集线程丢弃旧epoch的数据
        4. 主进程智能等待确保所有Agent进入新epoch
        5. 只清空旧epoch的统计数据
        6. 重置主统计对象
        """
        logger.info("Resetting stats (using epoch mechanism)...")

        # 1. 递增epoch（告诉所有Agent要重置了）
        old_epoch = self.reset_stats_flag.value
        new_epoch = old_epoch + 1
        self.reset_stats_flag.value = new_epoch
        logger.info(f"Stats reset: epoch {old_epoch} -> {new_epoch}")

        # 2. 智能等待所有Agent进入新epoch
        #    检查队列中收到的统计数据的epoch，确保所有Agent已响应
        if self.agent_processes:
            num_agents = len(self.agent_processes)
            agents_entered_new_epoch = set()
            max_wait_time = 2.0  # 最多等待2秒（Agent每秒发送统计）
            start_wait = time.time()

            logger.info(f"Waiting for {num_agents} agents to enter new epoch {new_epoch}...")

            while len(agents_entered_new_epoch) < num_agents:
                if time.time() - start_wait > max_wait_time:
                    logger.warning(
                        f"Timeout waiting for agents to enter new epoch. "
                        f"Only {len(agents_entered_new_epoch)}/{num_agents} agents confirmed."
                    )
                    break

                try:
                    # 非阻塞检查队列
                    stats_dict = self.stats_queue.get(timeout=0.1)
                    agent_id = stats_dict.get('agent_id')
                    stats_epoch = stats_dict.get('epoch', 0)

                    if stats_epoch >= new_epoch:
                        agents_entered_new_epoch.add(agent_id)
                        logger.debug(f"Agent {agent_id} entered epoch {stats_epoch} ({len(agents_entered_new_epoch)}/{num_agents})")
                    # 旧epoch的数据直接丢弃
                except:
                    # 队列空或超时，继续等待
                    pass

            if len(agents_entered_new_epoch) == num_agents:
                logger.info(f"All {num_agents} agents entered new epoch {new_epoch}")
            else:
                logger.warning(f"Only {len(agents_entered_new_epoch)}/{num_agents} agents confirmed new epoch")

        # 3. 清空queue中剩余的旧epoch数据（只清理旧epoch，保留新epoch数据）
        drained_old = 0
        drained_new = 0
        saved_new_epoch_stats = []

        try:
            while not self.stats_queue.empty():
                try:
                    stats_dict = self.stats_queue.get_nowait()
                    stats_epoch = stats_dict.get('epoch', 0)

                    if stats_epoch < new_epoch:
                        # 旧epoch数据，丢弃
                        drained_old += 1
                    else:
                        # 新epoch数据，保存并重新放回队列
                        saved_new_epoch_stats.append(stats_dict)
                        drained_new += 1
                except Exception as e:
                    logger.debug(f"Error draining queue: {e}")
                    break
        except Exception as e:
            logger.warning(f"Error while draining stats queue: {e}")

        # 将新epoch的统计数据放回队列
        for stats_dict in saved_new_epoch_stats:
            try:
                self.stats_queue.put_nowait(stats_dict)
            except Exception as e:
                logger.warning(f"Failed to restore new epoch stats to queue: {e}")

        if drained_old > 0 or drained_new > 0:
            logger.info(f"Drained {drained_old} old epoch entries, preserved {drained_new} new epoch entries")

        # 4. 重置主统计对象
        self.stats.reset()
        logger.info("Stats reset completed")

    def stop_all(self):
        """停止所有Agent进程和Consumers - 优雅关闭"""
        self.test_completed.set()

        # 1. 停止Agent进程
        if self.agent_processes:
            logger.info(f"Stopping {len(self.agent_processes)} Agent processes...")
            self.stop_agents.set()

            # 等待进程正常退出（给足够时间flush数据，最多10秒）
            start_time = time.time()
            all_stopped = True

            for process in self.agent_processes:
                remaining_time = max(0.1, 10 - (time.time() - start_time))
                process.join(timeout=remaining_time)
                if process.is_alive():
                    all_stopped = False

            if all_stopped:
                logger.info("All Agent processes exited gracefully")
            else:
                # 还有进程在运行，再给3秒宽限期
                alive_processes = [p for p in self.agent_processes if p.is_alive()]
                logger.warning(f"{len(alive_processes)} Agent processes still running, giving 3s grace period...")
                time.sleep(3.0)

                # 检查是否还有进程
                alive_processes = [p for p in self.agent_processes if p.is_alive()]
                if alive_processes:
                    logger.warning(f"Force terminating {len(alive_processes)} Agent processes that didn't exit")
                    for process in alive_processes:
                        logger.warning(f"Terminating Agent process: {process.name} (PID: {process.pid})")
                        try:
                            process.terminate()
                        except:
                            pass

                    # 再等1秒让terminate生效
                    time.sleep(1.0)

                    # 如果还活着，强制kill
                    still_alive = [p for p in alive_processes if p.is_alive()]
                    if still_alive:
                        logger.error(f"Forcefully killing {len(still_alive)} unresponsive processes")
                        for process in still_alive:
                            try:
                                process.kill()
                            except:
                                pass

            self.agent_processes.clear()
            logger.info("All Agent processes stopped")

        # 2. 停止统计收集线程
        if self.stats_collector_running:
            self.stats_collector_running = False
            if self.stats_collector_thread and self.stats_collector_thread.is_alive():
                self.stats_collector_thread.join(timeout=2.0)
            logger.info("Stats collector stopped")

        # 3. 清空统计队列剩余数据
        drained_count = 0
        try:
            while not self.stats_queue.empty():
                self.stats_queue.get_nowait()
                drained_count += 1
        except Exception as e:
            logger.warning(f"Error while draining stats queue during stop_all: {e}")

        if drained_count > 0:
            logger.info(f"Drained {drained_count} entries from stats queue during cleanup")

        # 4. 关闭V1 Consumers（如果有）
        if self.consumers:
            logger.info("Closing V1 consumers...")
            for consumer in self.consumers:
                try:
                    consumer.close()
                except Exception as e:
                    logger.error(f"Error closing consumer: {e}")
            self.consumers.clear()

        # V2架构：Consumer在独立进程中，已通过stop_agents.set()停止

        self.producers.clear()
        self.consumer_metadata.clear()

    def id(self) -> str:
        """Get worker ID."""
        return "local-worker"

    def close(self):
        """Close worker and cleanup."""
        self.stop_all()

        if self.benchmark_driver is not None:
            try:
                self.benchmark_driver.close()
            except Exception as e:
                logger.error(f"Error closing driver: {e}")

    # ConsumerCallback interface implementation (V1兼容性保留)
    def message_received(self, payload: bytes, publish_timestamp_ms: int):
        """
        Callback when message is received (ConsumerCallback interface).

        V1架构：在主进程中的Consumer使用此callback
        V2架构：Consumer在独立进程中，不使用此callback（直接在进程内计算E2E延迟）

        :param payload: Message payload
        :param publish_timestamp_ms: Publish timestamp in milliseconds (from epoch)
        """
        import time
        # IMPORTANT: Use milliseconds (same as Java) to match Kafka timestamp
        receive_timestamp_ms = int(time.time() * 1000)  # Convert to milliseconds from epoch
        end_to_end_latency_ms = receive_timestamp_ms - publish_timestamp_ms if publish_timestamp_ms > 0 else 0
        # Record in milliseconds for stats
        self.stats.record_message_received(len(payload), end_to_end_latency_ms)
        logger.debug(f"[V1] Message received callback: payload_size={len(payload)}, e2e_latency_ms={end_to_end_latency_ms}, total_received={self.stats.total_messages_received.sum()}")
