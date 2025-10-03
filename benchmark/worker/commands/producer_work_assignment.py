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

from typing import List, Optional


class ProducerWorkAssignment:

    def __init__(self):
        self.payload_data: Optional[List[bytes]] = None
        self.publish_rate: float = 0.0
        self.key_distributor_type = None  # KeyDistributorType

    def with_publish_rate(self, publish_rate: float) -> 'ProducerWorkAssignment':
        copy = ProducerWorkAssignment()
        copy.key_distributor_type = self.key_distributor_type
        copy.payload_data = self.payload_data
        copy.publish_rate = publish_rate
        return copy
