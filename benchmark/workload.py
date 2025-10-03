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


class Workload:

    def __init__(self):
        self.name = None

        # Number of topics to create in the test.
        self.topics = 0

        # Number of partitions each topic will contain.
        self.partitions_per_topic = 0

        self.key_distributor = None  # KeyDistributorType.NO_KEY

        self.message_size = 0

        self.use_randomized_payloads = False
        self.random_bytes_ratio = 0.0
        self.randomized_payload_pool_size = 0

        self.payload_file = None

        self.subscriptions_per_topic = 0

        self.producers_per_topic = 0

        self.consumer_per_subscription = 0

        self.producer_rate = 0

        # If the consumer backlog is > 0, the generator will accumulate messages until the requested
        # amount of storage is retained and then it will start the consumers to drain it.
        #
        # The testDurationMinutes will be overruled to allow the test to complete when the consumer
        # has drained all the backlog and it's on par with the producer
        self.consumer_backlog_size_gb = 0

        # The ratio of the backlog that can remain and yet the backlog still be considered empty, and
        # thus the workload can complete at the end of the configured duration. In some systems it is not
        # feasible for the backlog to be drained fully and thus the workload will run indefinitely. In
        # such circumstances, one may be content to achieve a partial drain such as 99% of the backlog.
        # The value should be on somewhere between 0.0 and 1.0, where 1.0 indicates that the backlog
        # should be fully drained, and 0.0 indicates a best effort, where the workload will complete
        # after the specified time irrespective of how much of the backlog has been drained.
        self.backlog_drain_ratio = 1.0

        self.test_duration_minutes = 0

        self.warmup_duration_minutes = 1
