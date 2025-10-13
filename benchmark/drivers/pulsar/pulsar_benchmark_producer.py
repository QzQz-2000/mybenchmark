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
Pulsar Benchmark Producer
"""

import logging
from concurrent.futures import Future

logger = logging.getLogger(__name__)


class PulsarBenchmarkProducer:
    """
    Pulsar Benchmark Producer

    Wraps a Pulsar producer for benchmark use.
    """

    def __init__(self, producer):
        """
        Initialize the benchmark producer.

        :param producer: Pulsar producer instance
        """
        self.producer = producer

    def send_async(self, key: bytes, payload: bytes) -> Future:
        """
        Send a message asynchronously.

        :param key: Message key (optional, can be None)
        :param payload: Message payload
        :return: Future that completes when message is acknowledged
        """
        future = Future()

        def callback(res, msg_id):
            if res == 0:  # pulsar.Result.Ok
                future.set_result(None)
            else:
                future.set_exception(Exception(f"Send failed with result code: {res}"))

        try:
            if key:
                self.producer.send_async(
                    content=payload,
                    partition_key=key.decode('utf-8') if isinstance(key, bytes) else key,
                    callback=callback
                )
            else:
                self.producer.send_async(
                    content=payload,
                    callback=callback
                )
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            future.set_exception(e)

        return future

    def close(self):
        """Close the producer."""
        if self.producer:
            self.producer.close()
