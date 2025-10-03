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

from typing import Optional
from concurrent.futures import Future
from kafka import KafkaProducer
from benchmark.driver.benchmark_producer import BenchmarkProducer


class KafkaBenchmarkProducer(BenchmarkProducer):
    """Kafka implementation of BenchmarkProducer."""

    def __init__(self, producer: KafkaProducer, topic: str):
        """
        Initialize Kafka benchmark producer.

        :param producer: Kafka producer instance
        :param topic: Topic name
        """
        self.producer = producer
        self.topic = topic

    def send_async(self, key: Optional[str], payload: bytes) -> Future:
        """
        Send message asynchronously.

        :param key: Message key (optional)
        :param payload: Message payload
        :return: Future that completes when message is sent
        """
        future = Future()

        def on_send_success(metadata):
            future.set_result(None)

        def on_send_error(exception):
            future.set_exception(exception)

        # Send to Kafka
        kafka_future = self.producer.send(
            self.topic,
            key=key.encode('utf-8') if key else None,
            value=payload
        )

        # Attach callbacks
        kafka_future.add_callback(on_send_success)
        kafka_future.add_errback(on_send_error)

        return future

    def close(self):
        """Close the producer."""
        self.producer.close()
