from abc import ABC, abstractmethod
from typing import List
from .commands.consumer_assignment import ConsumerAssignment
from .commands.counters_stats import CountersStats
from .commands.cumulative_latencies import CumulativeLatencies
from .commands.period_stats import PeriodStats
from .commands.producer_work_assignment import ProducerWorkAssignment
from .commands.topics_info import TopicsInfo


class Worker(ABC):
    """
    Worker interface for benchmark operations.
    Equivalent to AutoCloseable in Java.
    """

    @abstractmethod
    def initialize_driver(self, configuration_file: str):
        """Initialize the benchmark driver with configuration."""
        pass

    @abstractmethod
    def create_topics(self, topics_info: TopicsInfo) -> List[str]:
        """Create topics and return the list of topic names."""
        pass

    @abstractmethod
    def create_producers(self, topics: List[str]):
        """Create producers for the given topics."""
        pass

    @abstractmethod
    def create_consumers(self, consumer_assignment: ConsumerAssignment):
        """Create consumers based on assignment."""
        pass

    @abstractmethod
    def probe_producers(self):
        """Probe producers to ensure they are ready."""
        pass

    @abstractmethod
    def start_load(self, producer_work_assignment: ProducerWorkAssignment):
        """Start the load generation."""
        pass

    @abstractmethod
    def adjust_publish_rate(self, publish_rate: float):
        """Adjust the publishing rate."""
        pass

    @abstractmethod
    def pause_consumers(self):
        """Pause all consumers."""
        pass

    @abstractmethod
    def resume_consumers(self):
        """Resume all consumers."""
        pass

    @abstractmethod
    def get_counters_stats(self) -> CountersStats:
        """Get current counter statistics."""
        pass

    @abstractmethod
    def get_period_stats(self) -> PeriodStats:
        """Get period statistics."""
        pass

    @abstractmethod
    def get_cumulative_latencies(self) -> CumulativeLatencies:
        """Get cumulative latency statistics."""
        pass

    @abstractmethod
    def reset_stats(self):
        """Reset all statistics."""
        pass

    @abstractmethod
    def stop_all(self):
        """Stop all operations."""
        pass

    @abstractmethod
    def id(self) -> str:
        """Get worker ID."""
        pass

    @abstractmethod
    def close(self):
        """Close and cleanup resources."""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
