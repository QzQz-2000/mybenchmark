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
Pulsar Benchmark Consumer
"""

import logging

logger = logging.getLogger(__name__)


class PulsarBenchmarkConsumer:
    """
    Pulsar Benchmark Consumer

    Wraps a Pulsar consumer for benchmark use.
    """

    def __init__(self, consumer):
        """
        Initialize the benchmark consumer.

        :param consumer: Pulsar consumer instance
        """
        self.consumer = consumer
        self.paused = False

    def pause(self):
        """Pause the consumer."""
        self.paused = True
        if self.consumer:
            self.consumer.pause_message_listener()

    def resume(self):
        """Resume the consumer."""
        self.paused = False
        if self.consumer:
            self.consumer.resume_message_listener()

    def close(self):
        """Close the consumer."""
        if self.consumer:
            self.consumer.close()
