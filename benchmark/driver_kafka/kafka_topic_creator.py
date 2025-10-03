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

import time
import logging
import threading
from typing import List, Dict
from queue import Queue
from concurrent.futures import Future
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logger = logging.getLogger(__name__)


class KafkaTopicCreator:
    """Kafka topic creator with batching and retry support."""

    MAX_BATCH_SIZE = 500

    def __init__(
        self,
        admin: KafkaAdminClient,
        topic_configs: Dict[str, str],
        replication_factor: int,
        max_batch_size: int = MAX_BATCH_SIZE
    ):
        """
        Initialize Kafka topic creator.

        :param admin: Kafka admin client
        :param topic_configs: Topic configuration
        :param replication_factor: Replication factor
        :param max_batch_size: Maximum batch size
        """
        self.admin = admin
        self.topic_configs = topic_configs
        self.replication_factor = replication_factor
        self.max_batch_size = max_batch_size
        self._stop_logging = False

    def create(self, topic_infos: List) -> Future:
        """
        Create topics asynchronously.

        :param topic_infos: List of TopicInfo objects
        :return: Future that completes when all topics are created
        """
        future = Future()
        thread = threading.Thread(target=self._create_blocking_wrapper, args=(topic_infos, future))
        thread.start()
        return future

    def _create_blocking_wrapper(self, topic_infos: List, result_future: Future):
        """Wrapper to run blocking creation and set future result."""
        try:
            self._create_blocking(topic_infos)
            result_future.set_result(None)
        except Exception as e:
            result_future.set_exception(e)

    def _create_blocking(self, topic_infos: List):
        """
        Create topics in blocking mode with batching and retry.

        :param topic_infos: List of TopicInfo objects
        """
        queue = Queue()
        for topic_info in topic_infos:
            queue.put(topic_info)

        batch = []
        succeeded = 0

        # Start periodic logging
        self._stop_logging = False
        logging_thread = threading.Thread(
            target=self._periodic_logging,
            args=(lambda: succeeded, len(topic_infos)),
            daemon=True
        )
        logging_thread.start()

        try:
            while succeeded < len(topic_infos):
                # Drain queue to batch
                batch_size = 0
                while batch_size < self.max_batch_size and not queue.empty():
                    try:
                        batch.append(queue.get_nowait())
                        batch_size += 1
                    except:
                        break

                if batch_size > 0:
                    result_map = self._execute_batch(batch)
                    for topic_info, success in result_map.items():
                        if success:
                            succeeded += 1
                        else:
                            # Re-queue failed topic
                            queue.put(topic_info)
                    batch.clear()

        finally:
            self._stop_logging = True
            logging_thread.join(timeout=1)

    def _execute_batch(self, batch: List) -> Dict:
        """
        Execute a batch of topic creation requests.

        :param batch: Batch of TopicInfo objects
        :return: Map of TopicInfo to success status
        """
        logger.debug(f"Executing batch, size: {len(batch)}")

        # Create lookup map
        lookup = {info.topic: info for info in batch}

        # Create NewTopic objects
        new_topics = [self._new_topic(info) for info in batch]

        result_map = {}

        try:
            # Create topics
            create_result = self.admin.create_topics(new_topics, validate_only=False)

            # Check results
            for topic_name in create_result.topic_errors:
                topic_info = lookup.get(topic_name)
                if topic_info:
                    result_map[topic_info] = True

        except TopicAlreadyExistsError:
            # Topic already exists - treat as success
            for topic_info in batch:
                result_map[topic_info] = True
        except Exception as e:
            logger.debug(f"Error creating topics: {e}")
            # Mark all as failed for retry
            for topic_info in batch:
                result_map[topic_info] = False

        return result_map

    def _new_topic(self, topic_info) -> NewTopic:
        """
        Create NewTopic object.

        :param topic_info: TopicInfo object
        :return: NewTopic object
        """
        return NewTopic(
            name=topic_info.topic,
            num_partitions=topic_info.partitions,
            replication_factor=self.replication_factor,
            topic_configs=self.topic_configs
        )

    def _periodic_logging(self, get_succeeded, total: int):
        """Periodically log creation progress."""
        while not self._stop_logging:
            logger.info(f"Created topics {get_succeeded()}/{total}")
            time.sleep(10)
