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
import threading
import importlib
from typing import List
from concurrent.futures import ThreadPoolExecutor
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
        self.executor = ThreadPoolExecutor(thread_name_prefix="local-worker")
        self.stats = WorkerStats(stats_logger)
        self.test_completed = False
        self.consumers_are_paused = False
        self._lock = threading.Lock()
        self.producer_threads = []
        self.producer_work_assignment = None
        self.stop_producing = threading.Event()

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

    def start_load(self, producer_work_assignment: ProducerWorkAssignment):
        """Start load generation."""
        self._update_message_producer(producer_work_assignment.publish_rate)
        self.producer_work_assignment = producer_work_assignment
        self.stop_producing.clear()

        logger.info(f"Starting load at rate: {producer_work_assignment.publish_rate} msg/s")

        # Start producer threads - one thread per producer
        for i, producer in enumerate(self.producers):
            thread = threading.Thread(
                target=self._producer_worker,
                args=(producer, i),
                name=f"producer-{i}",
                daemon=True
            )
            thread.start()
            self.producer_threads.append(thread)

    def _producer_worker(self, producer, producer_index: int):
        """Worker thread that continuously sends messages."""
        import random

        work = self.producer_work_assignment
        if not work or not work.payload_data:
            return

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

                # Send message with rate limiting
                self.message_producer.send_message(producer, key, payload)

            except Exception as e:
                logger.error(f"Error in producer worker: {e}", exc_info=True)
                time.sleep(0.1)

    def adjust_publish_rate(self, publish_rate: float):
        """Adjust publishing rate."""
        self._update_message_producer(publish_rate)
        logger.info(f"Adjusted publish rate to: {publish_rate} msg/s")

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
        """Reset all statistics."""
        self.stats.reset()

    def stop_all(self):
        """Stop all producers and consumers."""
        self.test_completed = True

        # Stop producer threads
        self.stop_producing.set()
        for thread in self.producer_threads:
            if thread.is_alive():
                thread.join(timeout=1.0)
        self.producer_threads.clear()

        # Close producers
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
        :param publish_timestamp: Publish timestamp in microseconds
        """
        import time
        receive_timestamp = time.perf_counter_ns() // 1000  # Convert to microseconds
        end_to_end_latency = receive_timestamp - publish_timestamp if publish_timestamp > 0 else 0
        self.stats.record_message_received(len(payload), end_to_end_latency)
