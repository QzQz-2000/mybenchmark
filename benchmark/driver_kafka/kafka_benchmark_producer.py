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

from confluent_kafka import Producer
from benchmark.driver.benchmark_producer import BenchmarkProducer


class KafkaBenchmarkProducer(BenchmarkProducer):
    """Kafka producer implementation using confluent-kafka."""

    def __init__(self, topic: str, properties: dict):
        self.topic = topic
        self.producer = Producer(properties)

    def send_async(self, key: str, payload: bytes):
        """
        Send message asynchronously.

        :param key: Message key (optional)
        :param payload: Message payload
        :return: Future-like object
        """
        class FutureResult:
            def __init__(self):
                self.completed = False
                self.exception_value = None
                self._result = None
                self._callbacks = []

            def set_result(self, result=None):
                self._result = result
                self.completed = True
                self._run_callbacks()

            def set_exception(self, exc):
                self.exception_value = exc
                self.completed = True
                self._run_callbacks()

            def exception(self):
                return self.exception_value

            def result(self):
                if self.exception_value:
                    raise self.exception_value
                return self._result

            def add_done_callback(self, fn):
                if self.completed:
                    fn(self)
                else:
                    self._callbacks.append(fn)

            def _run_callbacks(self):
                for callback in self._callbacks:
                    try:
                        callback(self)
                    except:
                        pass
                self._callbacks.clear()

        future = FutureResult()

        def delivery_callback(err, msg):
            if err:
                future.set_exception(err)
            else:
                future.set_result()

        try:
            # Send message
            self.producer.produce(
                topic=self.topic,
                key=key.encode('utf-8') if key else None,
                value=payload,
                callback=delivery_callback
            )

            # Poll to handle callbacks (non-blocking)
            self.producer.poll(0)

        except Exception as e:
            future.set_exception(e)

        return future

    def close(self):
        """Close producer and flush pending messages."""
        try:
            # Flush all pending messages (wait up to 10 seconds)
            self.producer.flush(10)
        except:
            pass
