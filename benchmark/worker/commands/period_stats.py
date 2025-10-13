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

from hdrh.histogram import HdrHistogram


class PeriodStats:

    def __init__(self):
        self.messages_sent = 0
        self.message_send_errors = 0
        self.bytes_sent = 0

        self.messages_received = 0
        self.bytes_received = 0

        self.total_messages_sent = 0
        self.total_message_send_errors = 0
        self.total_messages_received = 0

        # 60 seconds in milliseconds, 5 significant digits
        self.publish_latency = HdrHistogram(1, 60 * 1_000, 5)
        # 60 seconds in milliseconds, 5 significant digits
        self.publish_delay_latency = HdrHistogram(1, 60 * 1_000, 5)
        # 12 hours in milliseconds, 5 significant digits
        self.end_to_end_latency = HdrHistogram(1, 12 * 60 * 60 * 1_000, 5)

    def plus(self, to_add: 'PeriodStats') -> 'PeriodStats':
        result = PeriodStats()

        result.messages_sent += self.messages_sent
        result.message_send_errors += self.message_send_errors
        result.bytes_sent += self.bytes_sent
        result.messages_received += self.messages_received
        result.bytes_received += self.bytes_received
        result.total_messages_sent += self.total_messages_sent
        result.total_message_send_errors += self.total_message_send_errors
        result.total_messages_received += self.total_messages_received
        result.publish_latency.add(self.publish_latency)
        result.publish_delay_latency.add(self.publish_delay_latency)
        result.end_to_end_latency.add(self.end_to_end_latency)

        result.messages_sent += to_add.messages_sent
        result.message_send_errors += to_add.message_send_errors
        result.bytes_sent += to_add.bytes_sent
        result.messages_received += to_add.messages_received
        result.bytes_received += to_add.bytes_received
        result.total_messages_sent += to_add.total_messages_sent
        result.total_message_send_errors += to_add.total_message_send_errors
        result.total_messages_received += to_add.total_messages_received
        result.publish_latency.add(to_add.publish_latency)
        result.publish_delay_latency.add(to_add.publish_delay_latency)
        result.end_to_end_latency.add(to_add.end_to_end_latency)

        return result
