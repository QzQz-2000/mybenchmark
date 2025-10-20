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

import requests
import json
import logging
from typing import List
from .worker import Worker
from .commands.consumer_assignment import ConsumerAssignment
from .commands.counters_stats import CountersStats
from .commands.cumulative_latencies import CumulativeLatencies
from .commands.period_stats import PeriodStats
from .commands.producer_work_assignment import ProducerWorkAssignment
from .commands.topics_info import TopicsInfo

logger = logging.getLogger(__name__)


class HttpWorkerClient(Worker):
    """
    HTTP client for remote worker communication.
    """

    def __init__(self, worker_address: str, default_timeout: int = 30):
        """
        Initialize HTTP worker client.

        :param worker_address: Worker address (e.g., "http://localhost:8080")
        :param default_timeout: Default timeout for HTTP requests in seconds (default: 30)
        """
        self.worker_address = worker_address.rstrip('/')
        self.session = requests.Session()
        self.default_timeout = default_timeout

    def initialize_driver(self, configuration_file: str):
        """Initialize driver on remote worker."""
        with open(configuration_file, 'rb') as f:
            files = {'file': f}
            response = self.session.post(f"{self.worker_address}/initialize-driver", files=files, timeout=self.default_timeout)
            response.raise_for_status()

    def create_topics(self, topics_info: TopicsInfo) -> List[str]:
        """Create topics on remote worker."""
        data = {
            'numberOfTopics': topics_info.number_of_topics,
            'numberOfPartitionsPerTopic': topics_info.number_of_partitions_per_topic
        }
        response = self.session.post(f"{self.worker_address}/create-topics", json=data, timeout=self.default_timeout)
        response.raise_for_status()
        return response.json()

    def delete_topics(self, topics: List[str]):
        """Delete topics on remote worker."""
        data = {'topics': topics}
        response = self.session.post(f"{self.worker_address}/delete-topics", json=data, timeout=self.default_timeout)
        response.raise_for_status()

    def create_producers(self, topics: List[str]):
        """Create producers on remote worker."""
        response = self.session.post(f"{self.worker_address}/create-producers", json=topics, timeout=self.default_timeout)
        response.raise_for_status()

    def create_consumers(self, consumer_assignment: ConsumerAssignment):
        """Create consumers on remote worker."""
        data = {
            'topicsSubscriptions': [
                {'topic': ts.topic, 'subscription': ts.subscription}
                for ts in consumer_assignment.topics_subscriptions
            ]
        }
        response = self.session.post(f"{self.worker_address}/create-consumers", json=data, timeout=self.default_timeout)
        response.raise_for_status()

    def probe_producers(self):
        """Probe producers on remote worker."""
        response = self.session.post(f"{self.worker_address}/probe-producers", timeout=self.default_timeout)
        response.raise_for_status()

    def start_load(self, producer_work_assignment: ProducerWorkAssignment, message_processing_delay_ms: int = 0):
        """Start load on remote worker."""
        data = {
            'publishRate': producer_work_assignment.publish_rate,
            'keyDistributorType': producer_work_assignment.key_distributor_type.value if producer_work_assignment.key_distributor_type else None,
            'payloadData': [list(payload) for payload in producer_work_assignment.payload_data] if producer_work_assignment.payload_data else [],
            'messageProcessingDelayMs': message_processing_delay_ms
        }
        response = self.session.post(f"{self.worker_address}/start-load", json=data, timeout=self.default_timeout)
        response.raise_for_status()

    def adjust_publish_rate(self, publish_rate: float):
        """Adjust publish rate on remote worker."""
        response = self.session.post(f"{self.worker_address}/adjust-rate", json={'publishRate': publish_rate}, timeout=self.default_timeout)
        response.raise_for_status()

    def pause_consumers(self):
        """Pause consumers on remote worker."""
        response = self.session.post(f"{self.worker_address}/pause-consumers", timeout=self.default_timeout)
        response.raise_for_status()

    def resume_consumers(self):
        """Resume consumers on remote worker."""
        response = self.session.post(f"{self.worker_address}/resume-consumers", timeout=self.default_timeout)
        response.raise_for_status()

    def get_counters_stats(self) -> CountersStats:
        """Get counter stats from remote worker."""
        response = self.session.get(f"{self.worker_address}/counters-stats", timeout=self.default_timeout)
        response.raise_for_status()
        data = response.json()

        stats = CountersStats()
        stats.messages_sent = data['messagesSent']
        stats.messages_received = data['messagesReceived']
        stats.message_send_errors = data['messageSendErrors']
        return stats

    def get_period_stats(self) -> PeriodStats:
        """Get period stats from remote worker."""
        response = self.session.get(f"{self.worker_address}/period-stats", timeout=self.default_timeout)
        response.raise_for_status()
        data = response.json()
        return PeriodStats.from_dict(data)

    def get_cumulative_latencies(self) -> CumulativeLatencies:
        """Get cumulative latencies from remote worker."""
        response = self.session.get(f"{self.worker_address}/cumulative-latencies", timeout=self.default_timeout)
        response.raise_for_status()
        data = response.json()
        return CumulativeLatencies.from_dict(data)

    def reset_stats(self):
        """Reset stats on remote worker."""
        response = self.session.post(f"{self.worker_address}/reset-stats", timeout=self.default_timeout)
        response.raise_for_status()

    def stop_all(self):
        """Stop all on remote worker."""
        try:
            response = self.session.post(f"{self.worker_address}/stop-all", timeout=5)
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"Error stopping remote worker: {e}")

    def start_producing(self):
        """Signal remote worker to start producing."""
        try:
            response = self.session.post(f"{self.worker_address}/start-producing", timeout=self.default_timeout)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Error starting production on remote worker: {e}")
            raise

    def stop_producing(self):
        """Signal remote worker to stop producing."""
        try:
            response = self.session.post(f"{self.worker_address}/stop-producing", timeout=self.default_timeout)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Error stopping production on remote worker: {e}")
            raise

    def id(self) -> str:
        """Get worker ID."""
        return self.worker_address

    def close(self):
        """Close HTTP session."""
        self.session.close()
