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

from typing import Optional


class TopicsInfo:

    def __init__(self, number_of_topics: Optional[int] = None,
                 number_of_partitions_per_topic: Optional[int] = None):
        self.number_of_topics = number_of_topics
        self.number_of_partitions_per_topic = number_of_partitions_per_topic
