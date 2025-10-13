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
from typing import Optional
from concurrent.futures import Future


class BenchmarkProducer(ABC):
    """
    BenchmarkProducer interface.
    Equivalent to AutoCloseable in Java.
    """

    # 异步发送消息
    @abstractmethod
    def send_async(self, key: Optional[str], payload: bytes) -> Future:
        """
        Publish a message and return a callback to track the completion of the operation.

        :param key: the key associated with this message
        :param payload: the message payload
        :return: a future that will be triggered when the message is successfully published
        """
        pass

    @abstractmethod
    def close(self):
        """Close the producer and cleanup resources."""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
