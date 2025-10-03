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

import logging
import multiprocessing
import threading
import time
import importlib
from typing import List
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from .worker import Worker
from .worker_stats import WorkerStats
from .message_producer import MessageProducer
from .commands.consumer_assignment import ConsumerAssignment
from .commands.counters_stats import CountersStats
from .commands.cumulative_latencies import CumulativeLatencies
from .commands.period_stats import PeriodStats
from .commands.producer_work_assignment import ProducerWorkAssignment
from .commands.topics_info import TopicsInfo

logger = logging.getLogger(__name__)


def _producer_agent_worker(agent_id, producer_infos, producer_properties, work_assignment,
                          stats_queue, shared_rate, stop_event):
    """
    Multi-agent worker function - each agent (process) handles multiple producers.
    Uses MessageProducer and WorkerStats for proper latency tracking.

    :param agent_id: Agent (process) ID
    :param producer_infos: List of (producer_index, topic) tuples for this agent
    :param producer_properties: Kafka producer configuration
    :param work_assignment: ProducerWorkAssignment with payload and settings
    :param stats_queue: Queue for sending statistics to main process (serialized format)
    :param shared_rate: multiprocessing.Value for dynamic rate adjustment
    :param stop_event: Event to signal shutdown
    """
    import random
    import time
    from benchmark.utils.uniform_rate_limiter import UniformRateLimiter
    from benchmark.worker.message_producer import MessageProducer
    from benchmark.worker.worker_stats import WorkerStats
    from benchmark.driver_kafka.kafka_benchmark_producer import KafkaBenchmarkProducer

    logger_local = logging.getLogger(__name__)
    logger_local.info(f"Agent {agent_id} started with {len(producer_infos)} producers")

    if not work_assignment or not work_assignment.payload_data:
        return

    # Create producers for this agent
    producers = []
    for producer_index, topic in producer_infos:
        producer = KafkaBenchmarkProducer(topic, producer_properties)
        producers.append((producer_index, producer))

    # Create local WorkerStats for this agent (thread-safe, not process-shared)
    local_stats = WorkerStats()

    # Get payload
    payload = work_assignment.payload_data[0] if work_assignment.payload_data else bytes(1024)

    # Rate limiting: total rate for this agent
    current_rate = shared_rate.value
    rate_limiter = UniformRateLimiter(current_rate)
    message_producer = MessageProducer(rate_limiter, local_stats)

    last_rate_check = time.time()
    last_stats_report = time.time()
    producer_cycle_index = 0

    # High-throughput send loop - no MessageProducer overhead
    logger_local.info(f"Agent {agent_id} target rate: {current_rate:.2f} msg/s")

    try:
        while not stop_event.is_set():
            try:
                # Check for rate adjustment
                now = time.time()
                if now - last_rate_check > 0.1:
                    new_rate = shared_rate.value
                    if abs(new_rate - current_rate) > 0.01:
                        current_rate = new_rate
                        rate_limiter = UniformRateLimiter(current_rate)
                        logger_local.info(f"Agent {agent_id} adjusted rate to {current_rate:.2f} msg/s")
                    last_rate_check = now

                # Report stats every 1 second
                if now - last_stats_report > 1.0:
                    try:
                        period_stats = local_stats.to_period_stats()
                        stats_dict = {
                            'agent_id': agent_id,
                            'messages_sent': period_stats.messages_sent,
                            'bytes_sent': period_stats.bytes_sent,
                            'errors': period_stats.message_send_errors,
                            'publish_latency': period_stats.publish_latency.encode() if period_stats.publish_latency else None,
                            'publish_delay_latency': period_stats.publish_delay_latency.encode() if period_stats.publish_delay_latency else None,
                            'timestamp': now
                        }
                        stats_queue.put_nowait(stats_dict)
                    except Exception as e:
                        logger_local.debug(f"Failed to send stats: {e}")
                    last_stats_report = now

                # Rate limiting - get next send time
                intended_send_time_ns = rate_limiter.acquire()

                # Busy-wait for high precision (for high rates)
                while time.perf_counter_ns() < intended_send_time_ns:
                    # Poll producers while waiting
                    for _, prod in producers:
                        prod.producer.poll(0)

                send_time_ns = time.perf_counter_ns()

                # Round-robin through producers
                producer_index, producer = producers[producer_cycle_index % len(producers)]
                producer_cycle_index += 1

                # Select key
                key = None
                if work_assignment.key_distributor_type and hasattr(work_assignment.key_distributor_type, 'name'):
                    if work_assignment.key_distributor_type.name == 'RANDOM':
                        key = str(random.randint(0, 1000000))
                    elif work_assignment.key_distributor_type.name == 'ROUND_ROBIN':
                        key = str(producer_index)

                # Send async
                future = producer.send_async(key, payload)

                # Attach callback for stats
                def record_stats(f, intended=intended_send_time_ns, sent=send_time_ns, payload_len=len(payload)):
                    now_ns = time.perf_counter_ns()
                    if f.exception():
                        local_stats.record_producer_failure()
                    else:
                        local_stats.record_producer_success(payload_len, intended, sent, now_ns)

                future.add_done_callback(record_stats)

            except Exception as e:
                local_stats.record_producer_failure()
                logger_local.error(f"Error in agent {agent_id}: {e}", exc_info=True)
                time.sleep(0.001)
    finally:
        # Close all producers
        for _, producer in producers:
            try:
                producer.close()
            except Exception as e:
                logger_local.error(f"Error closing producer: {e}")

        # Send final stats to main process
        try:
            counters = local_stats.to_counters_stats()
            stats_dict = {
                'agent_id': agent_id,
                'messages_sent': counters.messages_sent,
                'errors': counters.message_send_errors,
                'final': True
            }
            stats_queue.put(stats_dict)
        except Exception as e:
            logger_local.error(f"Error sending final stats: {e}")


class LocalWorker(Worker):
    """
    Local worker implementation that runs benchmark in the same process.
    Equivalent to Java's LocalWorker which implements Worker and ConsumerCallback.
    """

    def __init__(self, stats_logger=None):
        """
        Initialize local worker.

        :param stats_logger: Optional stats logger
        """
        self.benchmark_driver = None
        self.producers = []
        self.consumers = []
        self.message_producer = None
        self.executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="local-worker")
        self.stats = WorkerStats(stats_logger)
        self.test_completed = False
        self.consumers_are_paused = False
        self._lock = multiprocessing.Lock()
        self.producer_processes = []
        self.producer_work_assignment = None
        self.stop_producing = multiprocessing.Event()

        # Multi-agent architecture support
        self.stats_queue = multiprocessing.Queue()
        self.shared_publish_rate = multiprocessing.Value('d', 1.0)  # Shared rate for burst support

        # Initialize with default rate
        self._update_message_producer(1.0)

    def _update_message_producer(self, publish_rate: float):
        """Update message producer with new rate."""
        from benchmark.utils.uniform_rate_limiter import UniformRateLimiter
        rate_limiter = UniformRateLimiter(publish_rate)
        self.message_producer = MessageProducer(rate_limiter, self.stats)

    def initialize_driver(self, configuration_file: str):
        """Initialize the benchmark driver."""
        import yaml

        if self.benchmark_driver is not None:
            raise RuntimeError("Driver already initialized")

        self.test_completed = False

        # Load driver configuration
        with open(configuration_file, 'r') as f:
            driver_config = yaml.safe_load(f)

        logger.info(f"Driver: {driver_config}")

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
        self.benchmark_driver.create_topics(topic_infos)

        return topics

    def create_producers(self, topics: List[str]):
        """Create producers for topics."""
        if self.benchmark_driver is None:
            raise RuntimeError("Driver not initialized")

        # Create producer info objects
        class ProducerInfo:
            def __init__(self, id, topic):
                self.id = id
                self.topic = topic

        producer_infos = [
            ProducerInfo(i, topic)
            for i, topic in enumerate(topics)
        ]

        # Create producers (returns a Future)
        producers_future = self.benchmark_driver.create_producers(producer_infos)
        self.producers = producers_future.result()  # Wait for completion
        logger.info(f"Created {len(self.producers)} producers")

    def create_consumers(self, consumer_assignment: ConsumerAssignment):
        """Create consumers."""
        if self.benchmark_driver is None:
            raise RuntimeError("Driver not initialized")

        # Create consumer info objects
        class ConsumerInfo:
            def __init__(self, id, topic, subscription, callback):
                self.id = id
                self.topic = topic
                self.subscription_name = subscription
                self.consumer_callback = callback

        consumer_infos = [
            ConsumerInfo(i, ts.topic, ts.subscription, self)
            for i, ts in enumerate(consumer_assignment.topics_subscriptions)
        ]

        # Create consumers (returns a Future)
        consumers_future = self.benchmark_driver.create_consumers(consumer_infos)
        self.consumers = consumers_future.result()  # Wait for completion
        logger.info(f"Created {len(self.consumers)} consumers")

    def probe_producers(self):
        """Probe producers by sending one test message per producer."""
        for producer in self.producers:
            # Send a single test message
            producer.send_async(None, b"probe")

    def _assign_producers_to_agents(self, num_agents: int) -> dict:
        """
        Assign producers to agents using round-robin distribution.
        Matches Java's processorAssignment logic.

        :param num_agents: Number of agents (processes) to create
        :return: Dict mapping agent_id -> list of (producer_index, topic) tuples
        """
        agent_assignments = {i: [] for i in range(num_agents)}

        for producer_index, producer in enumerate(self.producers):
            agent_id = producer_index % num_agents
            topic = producer.topic  # Assume producer has topic attribute
            agent_assignments[agent_id].append((producer_index, topic))

        # Log assignment
        for agent_id, producers_info in agent_assignments.items():
            logger.info(f"Agent {agent_id}: {len(producers_info)} producers")

        return agent_assignments

    def start_load(self, producer_work_assignment: ProducerWorkAssignment):
        """
        Start load generation using multi-threaded architecture (like original).
        Each producer gets its own thread with the full target rate.
        """
        self.producer_work_assignment = producer_work_assignment
        self.stop_producing.clear()

        publish_rate = producer_work_assignment.publish_rate
        logger.info(f"Starting load generation: {len(self.producers)} producers at {publish_rate} msg/s each")

        # Update message producer with the target rate
        self._update_message_producer(publish_rate)

        # Create one thread per producer (like original implementation)
        self.producer_threads = []
        for i, producer in enumerate(self.producers):
            thread = threading.Thread(
                target=self._producer_worker_simple,
                args=(producer, i),
                name=f"producer-{i}",
                daemon=True
            )
            thread.start()
            self.producer_threads.append(thread)

        logger.info(f"Started {len(self.producer_threads)} producer threads")

    def _producer_worker_simple(self, producer, producer_index: int):
        """
        Worker thread that continuously sends messages (original approach).
        Each thread uses the shared MessageProducer with rate limiting.
        """
        import random

        work = self.producer_work_assignment
        if not work or not work.payload_data:
            logger.error("No work assignment or payload data available")
            return

        # Use first payload
        payload = work.payload_data[0] if work.payload_data else bytes(1024)

        logger.info(f"Producer worker {producer_index} started with payload size {len(payload)}")

        while not self.stop_producing.is_set():
            try:
                # Determine key based on distribution type
                key = None
                if work.key_distributor_type and hasattr(work.key_distributor_type, 'name'):
                    if work.key_distributor_type.name == 'RANDOM':
                        key = str(random.randint(0, 1000000))
                    elif work.key_distributor_type.name == 'ROUND_ROBIN':
                        key = str(producer_index)

                # Send message with rate limiting (MessageProducer handles stats)
                self.message_producer.send_message(producer, key, payload)

            except Exception as e:
                logger.error(f"Error in producer worker {producer_index}: {e}", exc_info=True)
                time.sleep(0.1)

        logger.info(f"Producer worker {producer_index} stopped")

    def _producer_worker(self, producer, producer_index: int, publish_rate: float):
        """Worker thread that continuously sends messages."""
        import random
        from benchmark.utils.uniform_rate_limiter import UniformRateLimiter

        work = self.producer_work_assignment
        if not work or not work.payload_data:
            return

        # Create independent rate limiter and message producer for this thread
        rate_limiter = UniformRateLimiter(publish_rate)
        message_producer = MessageProducer(rate_limiter, self.stats)

        # Get payload for this producer
        payload = work.payload_data[0] if work.payload_data else bytes(1024)

        while not self.stop_producing.is_set():
            try:
                # Select key based on key distributor
                key = None
                if work.key_distributor_type and hasattr(work.key_distributor_type, 'name'):
                    if work.key_distributor_type.name == 'RANDOM':
                        key = str(random.randint(0, 1000000))
                    elif work.key_distributor_type.name == 'ROUND_ROBIN':
                        key = str(producer_index)

                # Send message with rate limiting using dedicated message producer
                message_producer.send_message(producer, key, payload)

            except Exception as e:
                logger.error(f"Error in producer worker: {e}", exc_info=True)
                import time
                time.sleep(0.1)

    def adjust_publish_rate(self, publish_rate: float):
        """
        Adjust publishing rate - supports runtime burst scenarios.
        Updates shared_publish_rate which is monitored by all agent processes.
        """
        self.shared_publish_rate.value = publish_rate
        logger.info(f"Adjusted publish rate to: {publish_rate} msg/s (burst scenario supported)")

    def pause_consumers(self):
        """Pause all consumers."""
        with self._lock:
            self.consumers_are_paused = True
            for consumer in self.consumers:
                consumer.pause()

    def resume_consumers(self):
        """Resume all consumers."""
        with self._lock:
            self.consumers_are_paused = False
            for consumer in self.consumers:
                consumer.resume()

    def _collect_agent_stats(self):
        """Collect statistics from agent processes via queue and merge into main stats."""
        from hdrh.histogram import HdrHistogram

        try:
            while not self.stats_queue.empty():
                stats_dict = self.stats_queue.get_nowait()
                agent_id = stats_dict.get('agent_id')

                # These are period stats (already deltas from agent's to_period_stats())
                messages_sent = stats_dict.get('messages_sent', 0)
                bytes_sent = stats_dict.get('bytes_sent', 0)
                errors = stats_dict.get('errors', 0)

                # Add to main stats (period stats are additive)
                if messages_sent > 0:
                    self.stats.messages_sent.add(messages_sent)
                    self.stats.total_messages_sent.add(messages_sent)
                if bytes_sent > 0:
                    self.stats.bytes_sent.add(bytes_sent)
                    self.stats.total_bytes_sent.add(bytes_sent)
                if errors > 0:
                    self.stats.message_send_errors.add(errors)
                    self.stats.total_message_send_errors.add(errors)

                # Merge histogram data (encoded format)
                publish_latency_encoded = stats_dict.get('publish_latency')
                publish_delay_latency_encoded = stats_dict.get('publish_delay_latency')

                if publish_latency_encoded:
                    try:
                        agent_histogram = HdrHistogram.decode(publish_latency_encoded)
                        with self.stats.histogram_lock:
                            self.stats.publish_latency_recorder.add(agent_histogram)
                            self.stats.cumulative_publish_latency_recorder.add(agent_histogram)
                    except Exception as e:
                        logger.debug(f"Failed to merge publish latency histogram: {e}")

                if publish_delay_latency_encoded:
                    try:
                        agent_histogram = HdrHistogram.decode(publish_delay_latency_encoded)
                        with self.stats.histogram_lock:
                            self.stats.publish_delay_latency_recorder.add(agent_histogram)
                            self.stats.cumulative_publish_delay_latency_recorder.add(agent_histogram)
                    except Exception as e:
                        logger.debug(f"Failed to merge publish delay latency histogram: {e}")

                logger.debug(f"Agent {agent_id} stats: sent={messages_sent}, bytes={bytes_sent}, errors={errors}")
        except Exception as e:
            logger.error(f"Error collecting agent stats: {e}")

    def get_counters_stats(self) -> CountersStats:
        """Get counter statistics."""
        self._collect_agent_stats()
        return self.stats.to_counters_stats()

    def get_period_stats(self) -> PeriodStats:
        """Get period statistics."""
        self._collect_agent_stats()
        return self.stats.to_period_stats()

    def get_cumulative_latencies(self) -> CumulativeLatencies:
        """Get cumulative latencies."""
        self._collect_agent_stats()
        return self.stats.to_cumulative_latencies()

    def reset_stats(self):
        """Reset all statistics."""
        self.stats.reset()

    def stop_all(self):
        """Stop all producers and consumers."""
        self.test_completed = True

        # Stop producer threads (legacy support)
        self.stop_producing.set()
        if hasattr(self, 'producer_threads'):
            for thread in self.producer_threads:
                if thread.is_alive():
                    thread.join(timeout=1.0)
            self.producer_threads.clear()

        # Stop multi-agent producer processes
        if hasattr(self, 'producer_processes') and self.producer_processes:
            logger.info(f"Stopping {len(self.producer_processes)} agent processes")
            for process in self.producer_processes:
                if process.is_alive():
                    process.join(timeout=2.0)
                    if process.is_alive():
                        logger.warning(f"Force terminating agent process {process.name}")
                        process.terminate()
                        process.join(timeout=1.0)
            self.producer_processes.clear()

            # Collect final stats from agents
            self._collect_agent_stats()

        # Close producers (only if not using multi-agent mode, as agents handle their own)
        if not self.producer_processes:
            for producer in self.producers:
                try:
                    producer.close()
                except Exception as e:
                    logger.error(f"Error closing producer: {e}")

        # Close consumers
        for consumer in self.consumers:
            try:
                consumer.close()
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")

        self.producers.clear()
        self.consumers.clear()

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

        self.executor.shutdown(wait=True)

    # ConsumerCallback interface implementation
    def message_received(self, payload: bytes, publish_timestamp: int):
        """
        Callback when message is received (ConsumerCallback interface).

        :param payload: Message payload
        :param publish_timestamp: Publish timestamp in microseconds (from epoch)
        """
        import time
        # IMPORTANT: Use epoch time (not perf_counter) to match Kafka timestamp
        receive_timestamp = int(time.time() * 1_000_000)  # Convert to microseconds from epoch
        end_to_end_latency = receive_timestamp - publish_timestamp if publish_timestamp > 0 else 0
        self.stats.record_message_received(len(payload), end_to_end_latency)
