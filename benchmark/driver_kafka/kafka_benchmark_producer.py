# # Licensed under the Apache License, Version 2.0 (the "License");
# # you may not use this file except in compliance with the License.
# # You may obtain a copy of the License at
# #
# # http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing, software
# # distributed under the License is distributed on an "AS IS" BASIS,
# # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # See the License for the specific language governing permissions and
# # limitations under the License.

# import threading
# from confluent_kafka import Producer
# from benchmark.driver.benchmark_producer import BenchmarkProducer


# class KafkaBenchmarkProducer(BenchmarkProducer):
#     """Kafka producer implementation using confluent-kafka."""

#     def __init__(self, topic: str, properties: dict):
#         self.topic = topic
#         self.producer = Producer(properties)

#     def send_async(self, key: str, payload: bytes):
#         """
#         Send message asynchronously.

#         :param key: Message key (optional)
#         :param payload: Message payload
#         :return: Future-like object
#         """
#         class FutureResult:
#             def __init__(self):
#                 self.completed = False
#                 self.exception_value = None
#                 self._result = None
#                 self._callbacks = []
#                 self._lock = threading.Lock()  # Thread-safe lock

#             def set_result(self, result=None):
#                 with self._lock:
#                     self._result = result
#                     self.completed = True
#                     callbacks = self._callbacks[:]
#                     self._callbacks.clear()

#                 # Execute callbacks outside the lock to avoid deadlock
#                 for callback in callbacks:
#                     try:
#                         callback(self)
#                     except:
#                         pass

#             def set_exception(self, exc):
#                 with self._lock:
#                     self.exception_value = exc
#                     self.completed = True
#                     callbacks = self._callbacks[:]
#                     self._callbacks.clear()

#                 # Execute callbacks outside the lock to avoid deadlock
#                 for callback in callbacks:
#                     try:
#                         callback(self)
#                     except:
#                         pass

#             def exception(self):
#                 with self._lock:
#                     return self.exception_value

#             def result(self):
#                 with self._lock:
#                     if self.exception_value:
#                         raise self.exception_value
#                     return self._result

#             def add_done_callback(self, fn):
#                 should_call_now = False
#                 with self._lock:
#                     if self.completed:
#                         should_call_now = True
#                     else:
#                         self._callbacks.append(fn)

#                 # Call outside lock if already completed
#                 if should_call_now:
#                     try:
#                         fn(self)
#                     except:
#                         pass

#             def _run_callbacks(self):
#                 # This method is now unused, kept for compatibility
#                 pass

#         future = FutureResult()

#         def delivery_callback(err, msg):
#             if err:
#                 future.set_exception(err)
#             else:
#                 future.set_result()

#         try:
#             # Send message
#             self.producer.produce(
#                 topic=self.topic,
#                 key=key.encode('utf-8') if key else None,
#                 value=payload,
#                 callback=delivery_callback
#             )

#             # Poll to handle callbacks (non-blocking)
#             self.producer.poll(0)

#         except Exception as e:
#             future.set_exception(e)

#         return future

#     def close(self):
#         """Close producer and flush pending messages."""
#         try:
#             # Flush all pending messages (wait up to 10 seconds)
#             self.producer.flush(10)
#         except:
#             pass

# Licensed under the Apache License, Version 2.0
import threading
import time
from confluent_kafka import Producer
from benchmark.driver.benchmark_producer import BenchmarkProducer

class KafkaBenchmarkProducer(BenchmarkProducer):
    """Kafka producer implementation using confluent-kafka with timestamps."""

    def __init__(self, topic: str, properties: dict):
        self.topic = topic
        self.producer = Producer(properties)

    def send_async(self, key: str, payload: bytes):
        """
        Send message asynchronously with explicit timestamp.

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
                self._lock = threading.Lock()

            def set_result(self, result=None):
                with self._lock:
                    self._result = result
                    self.completed = True
                    callbacks = self._callbacks[:]
                    self._callbacks.clear()
                for callback in callbacks:
                    try:
                        callback(self)
                    except:
                        pass

            def set_exception(self, exc):
                with self._lock:
                    self.exception_value = exc
                    self.completed = True
                    callbacks = self._callbacks[:]
                    self._callbacks.clear()
                for callback in callbacks:
                    try:
                        callback(self)
                    except:
                        pass

            def exception(self):
                with self._lock:
                    return self.exception_value

            def result(self):
                with self._lock:
                    if self.exception_value:
                        raise self.exception_value
                    return self._result

            def add_done_callback(self, fn):
                should_call_now = False
                with self._lock:
                    if self.completed:
                        should_call_now = True
                    else:
                        self._callbacks.append(fn)
                if should_call_now:
                    try:
                        fn(self)
                    except:
                        pass

        future = FutureResult()

        def delivery_callback(err, msg):
            if err:
                future.set_exception(err)
            else:
                future.set_result()

        try:
            # Send message with explicit Producer timestamp
            self.producer.produce(
                topic=self.topic,
                key=key.encode('utf-8') if key else None,
                value=payload,
                timestamp=int(time.time() * 1000),  # 毫秒级时间戳
                callback=delivery_callback
            )

            # Poll to trigger callbacks
            self.producer.poll(0)

        except Exception as e:
            future.set_exception(e)

        return future

    def close(self):
        """Close producer and flush pending messages."""
        try:
            self.producer.flush(10)
        except:
            pass
