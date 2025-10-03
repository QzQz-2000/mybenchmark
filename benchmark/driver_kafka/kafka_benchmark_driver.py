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
from io import StringIO
from typing import List
from concurrent.futures import Future
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient
from benchmark.driver.benchmark_driver import BenchmarkDriver, TopicInfo
from benchmark.driver.benchmark_producer import BenchmarkProducer
from benchmark.driver.benchmark_consumer import BenchmarkConsumer
from benchmark.driver.consumer_callback import ConsumerCallback
from .config import Config
from .kafka_benchmark_producer import KafkaBenchmarkProducer
from .kafka_benchmark_consumer import KafkaBenchmarkConsumer
from .kafka_topic_creator import KafkaTopicCreator


class KafkaBenchmarkDriver(BenchmarkDriver):
    """Kafka implementation of BenchmarkDriver."""

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

        # Parse producer config
        self.producer_properties = dict(common_properties)
        self.producer_properties.update(self._parse_properties(self.config.producer_config))
        self.producer_properties['key_serializer'] = lambda k: k.encode('utf-8') if k else None
        self.producer_properties['value_serializer'] = lambda v: v

        # Parse consumer config
        self.consumer_properties = dict(common_properties)
        self.consumer_properties.update(self._parse_properties(self.config.consumer_config))
        self.consumer_properties['key_deserializer'] = lambda k: k.decode('utf-8') if k else None
        self.consumer_properties['value_deserializer'] = lambda v: v

        # Parse topic config
        self.topic_properties = self._parse_properties(self.config.topic_config)

        # Create admin client
        self.admin = KafkaAdminClient(**common_properties)

    def get_topic_name_prefix(self) -> str:
        """Get topic name prefix."""
        return "test-topic"

    def create_topic(self, topic: str, partitions: int) -> Future:
        """Create a single topic."""
        return self.create_topics([TopicInfo(topic, partitions)])

    def create_topics(self, topic_infos: List[TopicInfo]) -> Future:
        """Create multiple topics."""
        topic_creator = KafkaTopicCreator(
            self.admin,
            self.topic_properties,
            self.config.replication_factor
        )
        return topic_creator.create(topic_infos)

    def create_producer(self, topic: str) -> Future:
        """Create a producer."""
        future = Future()

        try:
            kafka_producer = KafkaProducer(**self.producer_properties)
            benchmark_producer = KafkaBenchmarkProducer(kafka_producer, topic)
            self.producers.append(benchmark_producer)
            future.set_result(benchmark_producer)
        except Exception as e:
            future.set_exception(e)

        return future

    def create_consumer(
        self,
        topic: str,
        subscription_name: str,
        consumer_callback: ConsumerCallback
    ) -> Future:
        """Create a consumer."""
        future = Future()

        try:
            properties = dict(self.consumer_properties)
            properties['group_id'] = subscription_name

            consumer = KafkaConsumer(topic, **properties)
            benchmark_consumer = KafkaBenchmarkConsumer(
                consumer,
                self.consumer_properties,
                consumer_callback
            )
            self.consumers.append(benchmark_consumer)
            future.set_result(benchmark_consumer)
        except Exception as e:
            future.set_exception(e)

        return future

    def close(self):
        """Close the driver."""
        for producer in self.producers:
            try:
                producer.close()
            except Exception:
                pass

        for consumer in self.consumers:
            try:
                consumer.close()
            except Exception:
                pass

        if self.admin:
            try:
                self.admin.close()
            except Exception:
                pass

    @staticmethod
    def _parse_properties(config_str: str) -> dict:
        """
        Parse properties from string.

        :param config_str: Properties string
        :return: Dictionary of properties
        """
        properties = {}
        if not config_str:
            return properties

        for line in config_str.strip().split('\n'):
            line = line.strip()
            if line and not line.startswith('#'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    # Convert Java-style dot notation to Python-style underscore
                    key = key.strip().replace('.', '_')
                    value = value.strip()

                    # Try to convert to appropriate type
                    # First try int, then float, then bool, otherwise keep as string
                    try:
                        value = int(value)
                    except ValueError:
                        try:
                            value = float(value)
                        except ValueError:
                            # Handle boolean values
                            if value.lower() == 'true':
                                value = True
                            elif value.lower() == 'false':
                                value = False

                    properties[key] = value

        return properties

    @staticmethod
    def _apply_zone_id(client_id: str, zone_id: str) -> str:
        """
        Replace zone ID template in client ID.

        :param client_id: Client ID with template
        :param zone_id: Zone ID value
        :return: Client ID with zone ID applied
        """
        return client_id.replace(KafkaBenchmarkDriver.ZONE_ID_TEMPLATE, zone_id)
