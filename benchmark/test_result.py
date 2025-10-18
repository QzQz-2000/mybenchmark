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

# 测试的一些指标
from typing import List, Dict


class TestResult:

    def __init__(self):
        self.workload = None
        self.driver = None
        self.message_size = 0
        self.topics = 0
        self.partitions = 0
        self.producers_per_topic = 0
        self.consumers_per_topic = 0

        self.publish_rate: List[float] = []
        self.publish_error_rate: List[float] = []
        self.consume_rate: List[float] = []
        self.backlog: List[int] = []

        # ⚡ Removed period avg lists - only percentiles tracked per period
        # Mean is calculated once at the end from complete histogram (aggregated_*_avg)
        self.publish_latency_50pct: List[float] = []
        self.publish_latency_75pct: List[float] = []
        self.publish_latency_95pct: List[float] = []
        self.publish_latency_99pct: List[float] = []
        self.publish_latency_999pct: List[float] = []
        self.publish_latency_9999pct: List[float] = []
        self.publish_latency_max: List[float] = []

        self.publish_delay_latency_50pct: List[int] = []
        self.publish_delay_latency_75pct: List[int] = []
        self.publish_delay_latency_95pct: List[int] = []
        self.publish_delay_latency_99pct: List[int] = []
        self.publish_delay_latency_999pct: List[int] = []
        self.publish_delay_latency_9999pct: List[int] = []
        self.publish_delay_latency_max: List[int] = []

        self.aggregated_publish_latency_avg = 0.0
        self.aggregated_publish_latency_50pct = 0.0
        self.aggregated_publish_latency_75pct = 0.0
        self.aggregated_publish_latency_95pct = 0.0
        self.aggregated_publish_latency_99pct = 0.0
        self.aggregated_publish_latency_999pct = 0.0
        self.aggregated_publish_latency_9999pct = 0.0
        self.aggregated_publish_latency_max = 0.0

        self.aggregated_publish_delay_latency_avg = 0.0
        self.aggregated_publish_delay_latency_50pct = 0
        self.aggregated_publish_delay_latency_75pct = 0
        self.aggregated_publish_delay_latency_95pct = 0
        self.aggregated_publish_delay_latency_99pct = 0
        self.aggregated_publish_delay_latency_999pct = 0
        self.aggregated_publish_delay_latency_9999pct = 0
        self.aggregated_publish_delay_latency_max = 0

        self.aggregated_publish_latency_quantiles: Dict[float, float] = {}

        self.aggregated_publish_delay_latency_quantiles: Dict[float, int] = {}

        # End to end latencies (from producer to consumer)
        # Latencies are expressed in milliseconds (without decimals)
        # ⚡ Removed period avg lists - only percentiles tracked per period
        self.end_to_end_latency_50pct: List[float] = []
        self.end_to_end_latency_75pct: List[float] = []
        self.end_to_end_latency_95pct: List[float] = []
        self.end_to_end_latency_99pct: List[float] = []
        self.end_to_end_latency_999pct: List[float] = []
        self.end_to_end_latency_9999pct: List[float] = []
        self.end_to_end_latency_max: List[float] = []

        self.aggregated_end_to_end_latency_quantiles: Dict[float, float] = {}

        self.aggregated_end_to_end_latency_avg = 0.0
        self.aggregated_end_to_end_latency_50pct = 0.0
        self.aggregated_end_to_end_latency_75pct = 0.0
        self.aggregated_end_to_end_latency_95pct = 0.0
        self.aggregated_end_to_end_latency_99pct = 0.0
        self.aggregated_end_to_end_latency_999pct = 0.0
        self.aggregated_end_to_end_latency_9999pct = 0.0
        self.aggregated_end_to_end_latency_max = 0.0

        # Aggregated throughput metrics (calculated from total messages / total time)
        self.aggregated_publish_rate_avg = 0.0  # Average producer throughput (msg/s)
        self.aggregated_consume_rate_avg = 0.0  # Average consumer throughput (msg/s)
        self.aggregated_publish_throughput_avg = 0.0  # Average producer throughput (MB/s)
        self.aggregated_consume_throughput_avg = 0.0  # Average consumer throughput (MB/s)
        self.aggregated_messages_sent = 0  # Total messages sent by producer
        self.aggregated_messages_received = 0  # Total messages received by consumer

    def get_topics(self) -> int:
        return self.topics

    def get_partitions(self) -> int:
        return self.partitions

    def get_message_size(self) -> int:
        return self.message_size
