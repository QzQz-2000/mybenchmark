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

from abc import ABC, abstractmethod
from typing import List
from concurrent.futures import Future
from dataclasses import dataclass
from .benchmark_producer import BenchmarkProducer
from .benchmark_consumer import BenchmarkConsumer
from .consumer_callback import ConsumerCallback


@dataclass
class TopicInfo:
    """Topic information."""
    topic: str
    partitions: int


@dataclass
class ProducerInfo:
    """Producer information."""
    id: int
    topic: str


@dataclass
class ConsumerInfo:
    """Consumer information."""
    id: int
    topic: str
    subscription_name: str
    consumer_callback: ConsumerCallback


class BenchmarkDriver(ABC):
    """
    Base driver interface.
    Equivalent to AutoCloseable in Java.
    """

    @abstractmethod
    def initialize(self, configuration_file: str, stats_logger):
        """
        Driver implementation can use this method to initialize the client libraries,
        with the provided configuration file.

        The format of the configuration file is specific to the driver implementation.

        :param configuration_file: Path to configuration file
        :param stats_logger: stats logger to collect stats from benchmark driver
        """
        pass

    # 生成多个topic的统一前缀
    @abstractmethod
    def get_topic_name_prefix(self) -> str:
        """
        Get a driver specific prefix to be used in creating multiple topic names.

        :return: the topic name prefix
        """
        pass

    # 创建单个producer
    @abstractmethod
    def create_topic(self, topic: str, partitions: int) -> Future:
        """
        Create a new topic with a given number of partitions.

        :param topic: Topic name
        :param partitions: Number of partitions
        :return: a future that completes when the topic is created
        """
        pass

    # 批量创建topics
    def create_topics(self, topic_infos: List[TopicInfo]) -> Future:
        """
        Create a list of new topics with the given number of partitions.

        :param topic_infos: List of topic information
        :return: a future that completes when the topics are created
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        futures = [self.create_topic(info.topic, info.partitions) for info in topic_infos]

        # Wait for all futures to complete
        result_future = Future()

        def wait_all():
            try:
                for future in as_completed(futures):
                    future.result()  # This will raise exception if any future failed
                result_future.set_result(None)
            except Exception as e:
                result_future.set_exception(e)

        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(wait_all)
        executor.shutdown(wait=False)

        return result_future

    @abstractmethod
    def create_producer(self, topic: str) -> Future:
        """
        Create a producer for a given topic.

        :param topic: Topic name
        :return: a producer future
        """
        pass

    def create_producers(self, producers: List[ProducerInfo]) -> Future:
        """
        Create producers for given topics.

        :param producers: List of producer information
        :return: a future that completes with list of producers
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        futures = [self.create_producer(info.topic) for info in producers]

        # Wait for all and collect results
        result_future = Future()

        def wait_all():
            try:
                results = []
                for future in as_completed(futures):
                    results.append(future.result())
                result_future.set_result(results)
            except Exception as e:
                result_future.set_exception(e)

        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(wait_all)
        executor.shutdown(wait=False)

        return result_future

    @abstractmethod
    def create_consumer(
        self,
        topic: str,
        subscription_name: str,
        consumer_callback: ConsumerCallback
    ) -> Future:
        """
        Create a benchmark consumer relative to one particular topic and subscription.

        It is responsibility of the driver implementation to invoke the consumerCallback
        each time a message is received.

        :param topic: Topic name
        :param subscription_name: Subscription name
        :param consumer_callback: Callback to invoke when message is received
        :return: a consumer future
        """
        pass

    def create_consumers(self, consumers: List[ConsumerInfo]) -> Future:
        """
        Create consumers for given topics.

        :param consumers: List of consumer information
        :return: a future that completes with list of consumers
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        futures = [
            self.create_consumer(info.topic, info.subscription_name, info.consumer_callback)
            for info in consumers
        ]

        # Wait for all and collect results
        result_future = Future()

        def wait_all():
            try:
                results = []
                for future in as_completed(futures):
                    results.append(future.result())
                result_future.set_result(results)
            except Exception as e:
                result_future.set_exception(e)

        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(wait_all)
        executor.shutdown(wait=False)

        return result_future

    @abstractmethod
    def close(self):
        """Close the driver and cleanup resources."""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
