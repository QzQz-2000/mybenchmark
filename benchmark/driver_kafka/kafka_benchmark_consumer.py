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
from confluent_kafka import Consumer, TopicPartition
from benchmark.driver.benchmark_consumer import BenchmarkConsumer
from benchmark.driver.consumer_callback import ConsumerCallback

logger = logging.getLogger(__name__)


def _consumer_loop_func(topic, properties, message_queue, poll_timeout, closing, paused):
    """Global consumer loop function for multiprocessing (must be at module level)."""
    # Create consumer in subprocess
    consumer = Consumer(properties)
    consumer.subscribe([topic])

    message_count = 0
    commit_interval = 100  # Commit every 100 messages

    try:
        while not closing.is_set():
            try:
                if paused.is_set():
                    # Pause all assigned partitions when paused
                    partitions = consumer.assignment()
                    if partitions:
                        consumer.pause(partitions)
                    import time
                    time.sleep(0.1)
                    continue
                else:
                    # Resume all assigned partitions when not paused
                    partitions = consumer.assignment()
                    if partitions:
                        consumer.resume(partitions)

                # Poll for messages
                msg = consumer.poll(timeout=poll_timeout)

                if msg is None:
                    continue

                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                # Send message to parent process via queue
                # Use message timestamp (in milliseconds, convert to microseconds)
                # timestamp()[0] = type: 0=not available, 1=create time, 2=log append time
                # timestamp()[1] = timestamp in milliseconds
                timestamp_type, timestamp_ms = msg.timestamp()
                if timestamp_type != 0:  # TIMESTAMP_NOT_AVAILABLE
                    timestamp_us = timestamp_ms * 1000
                else:
                    timestamp_us = 0
                message_queue.put((msg.value(), timestamp_us))

                # Commit offset periodically (every N messages)
                message_count += 1
                if message_count >= commit_interval:
                    try:
                        consumer.commit(asynchronous=True)
                        message_count = 0
                    except Exception as e:
                        logger.warning(f"Commit error: {e}")

            except Exception as e:
                logger.error(f"Exception in consumer loop: {e}", exc_info=True)
    finally:
        # Final commit before closing
        try:
            consumer.commit(asynchronous=False)
        except:
            pass
        consumer.close()


class KafkaBenchmarkConsumer(BenchmarkConsumer):
    """Kafka consumer implementation using confluent-kafka."""

    def __init__(
        self,
        topic: str,
        subscription_name: str,
        properties: dict,
        callback: ConsumerCallback,
        poll_timeout: float = 0.1
    ):
        """
        Initialize Kafka benchmark consumer.

        :param topic: Topic to subscribe to
        :param subscription_name: Consumer group name
        :param properties: Consumer properties
        :param callback: Callback to invoke when message is received
        :param poll_timeout: Poll timeout in seconds
        """
        # Set group.id from subscription_name
        properties['group.id'] = subscription_name

        self.topic = topic
        self.properties = properties
        self.callback = callback
        self.poll_timeout = poll_timeout
        self.closing = multiprocessing.Event()
        self.paused = multiprocessing.Event()
        self.message_queue = multiprocessing.Queue(maxsize=1000)

        # Start consumer process
        self.consumer_process = multiprocessing.Process(
            target=_consumer_loop_func,
            args=(self.topic, self.properties, self.message_queue, self.poll_timeout,
                  self.closing, self.paused),
            daemon=True
        )
        self.consumer_process.start()

        # Start callback thread to process messages from queue
        self.callback_thread = threading.Thread(target=self._callback_loop, daemon=True)
        self.callback_thread.start()

    def _callback_loop(self):
        """Loop to process messages from queue and invoke callback."""
        while not self.closing.is_set():
            try:
                # Get message from queue with timeout
                try:
                    payload, timestamp_us = self.message_queue.get(timeout=0.1)
                    self.callback.message_received(payload, timestamp_us)
                except:
                    # Queue empty or timeout
                    pass
            except Exception as e:
                logger.error(f"Exception in callback loop: {e}", exc_info=True)

    def pause(self):
        """Pause consuming."""
        self.paused.set()

    def resume(self):
        """Resume consuming."""
        self.paused.clear()

    def close(self):
        """Close the consumer."""
        self.closing.set()

        # Wait for callback thread
        if self.callback_thread.is_alive():
            self.callback_thread.join(timeout=2)

        # Wait for consumer process
        if self.consumer_process.is_alive():
            self.consumer_process.join(timeout=5)
            if self.consumer_process.is_alive():
                self.consumer_process.terminate()
