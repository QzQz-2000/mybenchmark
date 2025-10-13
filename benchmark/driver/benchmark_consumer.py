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

# 抽象类
class BenchmarkConsumer(ABC):
    """
    BenchmarkConsumer interface.
    Equivalent to AutoCloseable in Java.
    """

    @abstractmethod
    def close(self):
        """Close the consumer and cleanup resources."""
        pass

    # 当使用with时，返回对象本身
    def __enter__(self):
        return self

    # 当使用with时，结束时自动调用close()
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
