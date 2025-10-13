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


class CumulativeLatencies:

    def __init__(self):
        # 60 seconds in milliseconds, 5 significant digits
        self.publish_latency = HdrHistogram(1, 60 * 1_000, 5)
        # 60 seconds in milliseconds, 5 significant digits
        self.publish_delay_latency = HdrHistogram(1, 60 * 1_000, 5)
        # 12 hours in milliseconds, 5 significant digits
        self.end_to_end_latency = HdrHistogram(1, 12 * 60 * 60 * 1_000, 5)

    def plus(self, to_add: 'CumulativeLatencies') -> 'CumulativeLatencies':
        result = CumulativeLatencies()

        result.publish_latency.add(self.publish_latency)
        result.publish_delay_latency.add(self.publish_delay_latency)
        result.end_to_end_latency.add(self.end_to_end_latency)

        result.publish_latency.add(to_add.publish_latency)
        result.publish_delay_latency.add(to_add.publish_delay_latency)
        result.end_to_end_latency.add(to_add.end_to_end_latency)

        return result
