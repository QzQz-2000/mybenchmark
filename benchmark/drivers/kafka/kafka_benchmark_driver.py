import yaml
import os
import threading
from typing import List
from concurrent.futures import Future
from confluent_kafka.admin import AdminClient, NewTopic
from benchmark.driver.benchmark_driver import BenchmarkDriver, TopicInfo
from benchmark.driver.benchmark_producer import BenchmarkProducer
from benchmark.driver.benchmark_consumer import BenchmarkConsumer
from benchmark.driver.consumer_callback import ConsumerCallback
from .config import Config
from .kafka_benchmark_producer import KafkaBenchmarkProducer
from .kafka_benchmark_consumer import KafkaBenchmarkConsumer


class KafkaBenchmarkDriver(BenchmarkDriver):
    """Kafka implementation of BenchmarkDriver using confluent-kafka."""

    ZONE_ID_CONFIG = "zone.id"
    ZONE_ID_TEMPLATE = "{zone.id}"
    KAFKA_CLIENT_ID = "client.id"

    def __init__(self):
        self.config = None
        self.producers = []
        self.consumers = []
        self.topic_properties = {}
        self.producer_properties = {}
        self.consumer_properties = {}
        self.admin = None
        self.created_topics = []  # Track created topics for cleanup
        self._topics_lock = threading.Lock()  # Lock for thread-safe topic list access

    def initialize(self, configuration_file: str, stats_logger):
        """Initialize Kafka driver."""
        with open(configuration_file, 'r') as f:
            config_data = yaml.safe_load(f)

        self.config = Config()
        self.config.replication_factor = config_data.get('replicationFactor', 1)
        self.config.topic_config = config_data.get('topicConfig', '')
        self.config.common_config = config_data.get('commonConfig', '')
        self.config.producer_config = config_data.get('producerConfig', '')
        self.config.consumer_config = config_data.get('consumerConfig', '')

        # Parse common config
        common_properties = self._parse_properties(self.config.common_config)

        # Apply zone ID if present
        if self.KAFKA_CLIENT_ID in common_properties:
            zone_id = os.getenv(self.ZONE_ID_CONFIG, '')
            common_properties[self.KAFKA_CLIENT_ID] = self._apply_zone_id(
                common_properties[self.KAFKA_CLIENT_ID],
                zone_id
            )

        # Parse producer config (confluent-kafka uses dots in config names)
        self.producer_properties = dict(common_properties)
        producer_props = self._parse_properties_confluent(self.config.producer_config)
        self.producer_properties.update(producer_props)

        # Parse consumer config (confluent-kafka uses dots in config names)
        self.consumer_properties = dict(common_properties)
        consumer_props = self._parse_properties_confluent(self.config.consumer_config)
        self.consumer_properties.update(consumer_props)

        # Parse topic config
        self.topic_properties = self._parse_properties_confluent(self.config.topic_config)

        # Create admin client
        self.admin = AdminClient(common_properties)

    def get_topic_name_prefix(self) -> str:
        """Get topic name prefix."""
        return "test-topic"

    def get_producer_properties(self) -> dict:
        """Get producer properties."""
        return self.producer_properties.copy()

    def get_consumer_properties(self) -> dict:
        """Get consumer properties."""
        return self.consumer_properties.copy()

    def create_topic(self, topic: str, partitions: int) -> Future:
        """Create a single topic."""
        topic_info = TopicInfo(topic, partitions)
        return self.create_topics([topic_info])

    def create_topics(self, topic_infos: List) -> Future:
        """Create multiple topics - synchronous execution.

        Args:
            topic_infos: List of TopicInfo objects OR dicts with 'topic' and 'partitions' keys
        """
        import concurrent.futures
        import logging

        logger = logging.getLogger(__name__)
        future = concurrent.futures.Future()

        try:
            # Handle both TopicInfo objects and dicts
            def get_topic_name(info):
                return info.topic if hasattr(info, 'topic') else info['topic']

            def get_partitions(info):
                return info.partitions if hasattr(info, 'partitions') else info['partitions']

            topic_names = [get_topic_name(info) for info in topic_infos]
            logger.info(f"📝 Starting creation of {len(topic_infos)} topics: {topic_names}")
            logger.info(f"📝 Topic config properties: {self.topic_properties}")

            new_topics = [
                NewTopic(
                    get_topic_name(topic_info),
                    num_partitions=get_partitions(topic_info),
                    replication_factor=self.config.replication_factor,
                    config=self.topic_properties
                )
                for topic_info in topic_infos
            ]

            # Create topics
            fs = self.admin.create_topics(new_topics)

            # Wait for all topics to be created
            created_count = 0
            already_exists_count = 0
            for topic, f in fs.items():
                try:
                    f.result()  # Block until topic is created
                    partitions = get_partitions([info for info in topic_infos if get_topic_name(info) == topic][0])
                    logger.info(f"✅ Created topic: {topic} (partitions={partitions}, replication={self.config.replication_factor})")
                    created_count += 1
                    # Track created topic for cleanup (thread-safe)
                    with self._topics_lock:
                        self.created_topics.append(topic)
                except Exception as e:
                    error_msg = str(e)
                    # Topic正在删除中 - 需要抛出异常让上层重试
                    if "marked for deletion" in error_msg:
                        logger.error(f"❌ Failed to create topic {topic}: {e}")
                        raise
                    # Topic已经存在（且不是正在删除），这是正常情况
                    elif "already exists" in error_msg:
                        logger.info(f"⚠️  Topic {topic} already exists, skipping creation")
                        already_exists_count += 1
                        # Still track it if it already exists (thread-safe)
                        with self._topics_lock:
                            self.created_topics.append(topic)
                    else:
                        # 其他错误
                        logger.error(f"❌ Failed to create topic {topic}: {e}")
                        raise

            logger.info(f"📝 Topic creation complete: {created_count} created, {already_exists_count} already existed")
            future.set_result(None)
        except Exception as e:
            logger.error(f"❌ Failed to create topics: {e}")
            future.set_exception(e)

        return future

    def delete_topics(self, topic_names: List[str]) -> Future:
        """Delete multiple topics."""
        import concurrent.futures
        import logging

        logger = logging.getLogger(__name__)
        future = concurrent.futures.Future()

        try:
            if not topic_names:
                logger.info("🗑️  No topics to delete (empty list)")
                future.set_result(None)
                return future

            logger.info(f"🗑️  Starting deletion of {len(topic_names)} topics: {topic_names}")

            # Delete topics
            fs = self.admin.delete_topics(topic_names, operation_timeout=30)

            # Wait for all to complete
            deleted_count = 0
            for topic, f in fs.items():
                try:
                    f.result()  # Wait for the result
                    logger.info(f"✅ Deleted topic: {topic}")
                    deleted_count += 1
                except Exception as e:
                    # Ignore if topic doesn't exist
                    if 'UnknownTopicOrPartitionException' in str(e) or 'UNKNOWN_TOPIC_OR_PARTITION' in str(e):
                        logger.info(f"⚠️  Topic {topic} does not exist, skipping")
                    else:
                        logger.warning(f"⚠️  Error deleting topic {topic}: {e}")

            logger.info(f"🗑️  Deletion complete: {deleted_count}/{len(topic_names)} topics deleted")
            future.set_result(None)
        except Exception as e:
            logger.error(f"❌ Failed to delete topics: {e}")
            future.set_exception(e)

        return future

    def create_producer(self, topic: str) -> Future:
        """Create a single producer - synchronous execution."""
        import concurrent.futures

        future = concurrent.futures.Future()

        try:
            producer = KafkaBenchmarkProducer(
                topic,
                self.producer_properties.copy()
            )
            self.producers.append(producer)
            future.set_result(producer)
        except Exception as e:
            future.set_exception(e)

        return future

    def create_producers(self, producer_infos: List) -> Future:
        """Create producers - synchronous execution."""
        import concurrent.futures

        future = concurrent.futures.Future()

        try:
            producers = []
            for info in producer_infos:
                producer = KafkaBenchmarkProducer(
                    info.topic,
                    self.producer_properties.copy()
                )
                producers.append(producer)

            self.producers.extend(producers)
            future.set_result(producers)
        except Exception as e:
            future.set_exception(e)

        return future

    def create_consumer(self, topic: str, subscription_name: str, consumer_callback) -> Future:
        """Create a single consumer - synchronous execution."""
        import concurrent.futures

        future = concurrent.futures.Future()

        try:
            consumer = KafkaBenchmarkConsumer(
                topic,
                subscription_name,
                self.consumer_properties.copy(),
                consumer_callback
            )
            self.consumers.append(consumer)
            future.set_result(consumer)
        except Exception as e:
            future.set_exception(e)

        return future

    def create_consumers(self, consumer_infos: List) -> Future:
        """Create consumers - synchronous execution."""
        import concurrent.futures

        future = concurrent.futures.Future()

        try:
            consumers = []
            for info in consumer_infos:
                consumer = KafkaBenchmarkConsumer(
                    info.topic,
                    info.subscription_name,
                    self.consumer_properties.copy(),
                    info.consumer_callback
                )
                consumers.append(consumer)

            self.consumers.extend(consumers)
            future.set_result(consumers)
        except Exception as e:
            future.set_exception(e)

        return future

    def delete_all_created_topics(self):
        """Delete all created topics (for cleanup in close())."""
        import logging
        logger = logging.getLogger(__name__)

        with self._topics_lock:
            if not self.created_topics:
                logger.info("🗑️  No topics to delete (list is empty)")
                return

            topics_to_delete = self.created_topics[:]

        try:
            logger.info(f"🗑️  Deleting {len(topics_to_delete)} topics: {topics_to_delete}")
            fs = self.admin.delete_topics(topics_to_delete, operation_timeout=30)

            # Wait for deletion to complete
            deleted_count = 0
            for topic, f in fs.items():
                try:
                    f.result()
                    logger.info(f"✅ Successfully deleted topic: {topic}")
                    deleted_count += 1
                except Exception as e:
                    logger.warning(f"⚠️  Failed to delete topic {topic}: {e}")

            logger.info(f"🗑️  Deletion complete: {deleted_count}/{len(topics_to_delete)} topics deleted")

            with self._topics_lock:
                self.created_topics.clear()
        except Exception as e:
            logger.error(f"❌ Error deleting topics: {e}")

    def delete_all_created_consumer_groups(self):
        """
        Consumer groups are no longer deleted as each test run uses unique timestamped group names.
        This method is kept for backward compatibility but does nothing.
        """
        import logging
        logger = logging.getLogger(__name__)
        logger.info("ℹ️  Consumer groups are not deleted (using timestamped unique names per test run)")

    def close(self):
        """Close all resources and cleanup topics and consumer groups."""
        import logging
        import time
        logger = logging.getLogger(__name__)

        # Stop all producers
        for producer in self.producers:
            try:
                producer.close()
            except Exception as e:
                logger.warning(f"Error closing producer: {e}")

        # Stop all consumers
        for consumer in self.consumers:
            try:
                consumer.close()
            except Exception as e:
                logger.warning(f"Error closing consumer: {e}")

        self.producers.clear()
        self.consumers.clear()

        # Wait a bit for consumers to fully disconnect
        logger.info("⏳ Waiting 2 seconds for consumers to fully disconnect...")
        time.sleep(2)

        # Delete consumer groups first (must be done before topics are deleted)
        self.delete_all_created_consumer_groups()

        # Delete created topics for idempotency
        self.delete_all_created_topics()

    @staticmethod
    def _parse_properties_confluent(config_str: str) -> dict:
        """Parse properties for confluent-kafka (keeps dots in keys)."""
        properties = {}
        if not config_str:
            return properties

        for line in config_str.strip().split('\n'):
            line = line.strip()
            if line and not line.startswith('#'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()

                    # Try to convert to appropriate type
                    try:
                        value = int(value)
                    except ValueError:
                        try:
                            value = float(value)
                        except ValueError:
                            if value.lower() == 'true':
                                value = True
                            elif value.lower() == 'false':
                                value = False

                    properties[key] = value
        return properties

    @staticmethod
    def _parse_properties(config_str: str) -> dict:
        """Parse properties (legacy method, keeps for compatibility)."""
        return KafkaBenchmarkDriver._parse_properties_confluent(config_str)

    @staticmethod
    def _apply_zone_id(template: str, zone_id: str) -> str:
        """Apply zone ID to template string."""
        if not zone_id:
            return template
        return template.replace(KafkaBenchmarkDriver.ZONE_ID_TEMPLATE, zone_id)
