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
from typing import Dict
from kafka import KafkaConsumer
from kafka import TopicPartition, OffsetAndMetadata
from benchmark.driver.benchmark_consumer import BenchmarkConsumer
from benchmark.driver.consumer_callback import ConsumerCallback

logger = logging.getLogger(__name__)


class KafkaBenchmarkConsumer(BenchmarkConsumer):
    """Kafka implementation of BenchmarkConsumer."""

    def __init__(
        self,
        consumer: KafkaConsumer,
        consumer_config: Dict,
        callback: ConsumerCallback,
        poll_timeout_ms: int = 100
    ):
        """
        Initialize Kafka benchmark consumer.

        :param consumer: Kafka consumer instance
        :param consumer_config: Consumer configuration
        :param callback: Callback to invoke when message is received
        :param poll_timeout_ms: Poll timeout in milliseconds
        """
        self.consumer = consumer
        self.callback = callback
        self.poll_timeout_ms = poll_timeout_ms
        self.closing = False
        self.auto_commit = consumer_config.get('enable.auto.commit', 'false').lower() == 'true'

        # Start consumer thread
        self.consumer_thread = threading.Thread(target=self._consume_loop, daemon=True)
        self.consumer_thread.start()

    def _consume_loop(self):
        """Main consumer loop."""
        while not self.closing:
            try:
                records = self.consumer.poll(timeout_ms=self.poll_timeout_ms)

                offset_map = {}
                for topic_partition, messages in records.items():
                    for record in messages:
                        # Invoke callback
                        self.callback.message_received(record.value, record.timestamp)

                        # Track offset for manual commit
                        # OffsetAndMetadata(offset, metadata, leader_epoch) in kafka-python 2.x
                        offset_map[topic_partition] = OffsetAndMetadata(record.offset + 1, "", -1)

                # Manual commit if not auto-commit
                if not self.auto_commit and offset_map:
                    # Async commit all messages polled so far
                    self.consumer.commit_async(offsets=offset_map)

            except Exception as e:
                logger.error(f"Exception occurred while consuming message: {e}", exc_info=True)

    def close(self):
        """Close the consumer."""
        self.closing = True
        self.consumer_thread.join(timeout=5)
        self.consumer.close()
