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

from typing import List
from dataclasses import dataclass, field


@dataclass
class WorkloadSetTemplate:
    """
    A template that defines a set of workload definitions. This is much like the
    Workload entity, except that for many values that typically change in
    a benchmark, one can specify a sequence of values.
    """

    DEFAULT_NAME_TEMPLATE = (
        "${topics}-topics-${partitionsPerTopic}-partitions-${messageSize}b"
        "-${producersPerTopic}p-${consumerPerSubscription}c-${producerRate}"
    )

    name_format: str = DEFAULT_NAME_TEMPLATE

    # Number of topics to create in the test.
    topics: List[int] = field(default_factory=list)
    # Number of partitions each topic will contain.
    partitions_per_topic: List[int] = field(default_factory=list)

    message_size: List[int] = field(default_factory=list)
    subscriptions_per_topic: List[int] = field(default_factory=list)
    producers_per_topic: List[int] = field(default_factory=list)
    consumer_per_subscription: List[int] = field(default_factory=list)
    producer_rate: List[int] = field(default_factory=list)

    key_distributor: str = "NO_KEY"
    payload_file: str = None
    use_randomized_payloads: bool = False
    random_bytes_ratio: float = 0.0
    randomized_payload_pool_size: int = 0
    consumer_backlog_size_gb: int = 0
    test_duration_minutes: int = 5
    warmup_duration_minutes: int = 1
