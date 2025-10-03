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

import yaml
import os
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

    def create_topics(self, topic_infos: List[TopicInfo]) -> Future:
        """Create multiple topics - synchronous execution."""
        import concurrent.futures

        future = concurrent.futures.Future()

        try:
            new_topics = [
                NewTopic(
                    topic_info.topic,
                    num_partitions=topic_info.partitions,
                    replication_factor=self.config.replication_factor,
                    config=self.topic_properties
                )
                for topic_info in topic_infos
            ]

            # Create topics
            fs = self.admin.create_topics(new_topics)

            # Wait for all topics to be created
            for topic, f in fs.items():
                try:
                    f.result()  # Block until topic is created
                    # Track created topic for cleanup
                    self.created_topics.append(topic)
                except Exception as e:
                    # Topic might already exist, which is fine
                    if "already exists" not in str(e):
                        raise
                    else:
                        # Still track it if it already exists
                        self.created_topics.append(topic)

            future.set_result(None)
        except Exception as e:
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

    def delete_topics(self):
        """Delete all created topics."""
        import logging
        logger = logging.getLogger(__name__)

        if not self.created_topics:
            return

        try:
            logger.info(f"Deleting {len(self.created_topics)} topics: {self.created_topics}")
            fs = self.admin.delete_topics(self.created_topics, operation_timeout=30)

            # Wait for deletion to complete
            for topic, f in fs.items():
                try:
                    f.result()
                    logger.info(f"Successfully deleted topic: {topic}")
                except Exception as e:
                    logger.warning(f"Failed to delete topic {topic}: {e}")

            self.created_topics.clear()
        except Exception as e:
            logger.error(f"Error deleting topics: {e}")

    def close(self):
        """Close all resources and cleanup topics."""
        import logging
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

        # Delete created topics for idempotency
        self.delete_topics()

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
