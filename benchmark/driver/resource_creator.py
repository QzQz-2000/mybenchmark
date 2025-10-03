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
from typing import List, Dict, Callable, TypeVar, Generic
from queue import Queue
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass

logger = logging.getLogger(__name__)

R = TypeVar('R')  # Resource request type
C = TypeVar('C')  # Created resource type


@dataclass
class CreationResult(Generic[C]):
    """Result of resource creation."""
    created: C
    success: bool


class ResourceCreator(Generic[R, C]):
    """
    Generic resource creator with batching and retry support.
    """

    def __init__(
        self,
        name: str,
        max_batch_size: int,
        inter_batch_delay_ms: int,
        invoke_batch_fn: Callable[[List[R]], Dict[R, Future]],
        complete: Callable[[Future], CreationResult[C]]
    ):
        """
        Initialize resource creator.

        :param name: Name of the resource type (for logging)
        :param max_batch_size: Maximum batch size
        :param inter_batch_delay_ms: Delay between batches in milliseconds
        :param invoke_batch_fn: Function to invoke a batch of resource creation requests
        :param complete: Function to complete a future and extract the creation result
        """
        self.name = name
        self.max_batch_size = max_batch_size
        self.inter_batch_delay_ms = inter_batch_delay_ms
        self.invoke_batch_fn = invoke_batch_fn
        self.complete = complete
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="resource-creator")
        self._stop_logging = False

    def create(self, resources: List[R]) -> Future:
        """
        Create resources asynchronously.

        :param resources: List of resource requests
        :return: Future that completes with list of created resources
        """
        future = Future()
        self.executor.submit(self._create_blocking_wrapper, resources, future)
        return future

    def _create_blocking_wrapper(self, resources: List[R], result_future: Future):
        """Wrapper to run blocking creation and set future result."""
        try:
            result = self._create_blocking(resources)
            result_future.set_result(result)
        except Exception as e:
            result_future.set_exception(e)

    def _create_blocking(self, resources: List[R]) -> List[C]:
        """
        Create resources in blocking mode with batching and retry.

        :param resources: List of resource requests
        :return: List of created resources
        """
        queue = Queue()
        for resource in resources:
            queue.put(resource)

        batch = []
        created = []
        succeeded = 0

        # Start periodic logging
        self._stop_logging = False
        logging_thread = threading.Thread(
            target=self._periodic_logging,
            args=(lambda: succeeded, len(resources)),
            daemon=True
        )
        logging_thread.start()

        try:
            while succeeded < len(resources):
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
                    for resource, result in result_map.items():
                        if result.success:
                            created.append(result.created)
                            succeeded += 1
                        else:
                            # Re-queue failed resource
                            queue.put(resource)
                    batch.clear()

        finally:
            self._stop_logging = True
            logging_thread.join(timeout=1)

        return created

    def _execute_batch(self, batch: List[R]) -> Dict[R, CreationResult[C]]:
        """
        Execute a batch of resource creation requests.

        :param batch: Batch of resource requests
        :return: Map of resource to creation result
        """
        logger.debug(f"Executing batch, size: {len(batch)}")
        time.sleep(self.inter_batch_delay_ms / 1000.0)

        futures_map = self.invoke_batch_fn(batch)
        result_map = {}

        for resource, future in futures_map.items():
            result_map[resource] = self.complete(future)

        return result_map

    def _periodic_logging(self, get_succeeded: Callable[[], int], total: int):
        """Periodically log creation progress."""
        while not self._stop_logging:
            logger.info(f"Created {self.name}s {get_succeeded()}/{total}")
            time.sleep(10)
