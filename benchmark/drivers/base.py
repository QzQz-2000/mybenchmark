"""Abstract driver interface for messaging systems."""

import time
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, AsyncIterator
from dataclasses import dataclass
from ..core.config import DriverConfig
from ..core.results import ThroughputStats, LatencyStats, ErrorStats
from ..utils.logging import LoggerMixin


@dataclass
class Message:
    """Message data structure."""
    key: Optional[bytes] = None
    value: bytes = b""
    headers: Optional[Dict[str, bytes]] = None
    timestamp: Optional[float] = None


@dataclass
class ProducedMessage:
    """Message with production metadata."""
    message: Message
    send_time: float
    partition: Optional[int] = None
    offset: Optional[int] = None


@dataclass
class ConsumedMessage:
    """Message with consumption metadata."""
    message: Message
    topic: str
    partition: int
    offset: int
    receive_time: float


class AbstractProducer(ABC):
    """Abstract producer interface."""

    @abstractmethod
    async def send_message(self, topic: str, message: Message) -> ProducedMessage:
        """Send a single message.

        Args:
            topic: Target topic
            message: Message to send

        Returns:
            ProducedMessage with metadata
        """
        pass

    @abstractmethod
    async def send_batch(self, topic: str, messages: List[Message]) -> List[ProducedMessage]:
        """Send a batch of messages.

        Args:
            topic: Target topic
            messages: List of messages to send

        Returns:
            List of ProducedMessage with metadata
        """
        pass

    @abstractmethod
    async def flush(self) -> None:
        """Flush pending messages."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close the producer."""
        pass


class AbstractConsumer(ABC):
    """Abstract consumer interface."""

    @abstractmethod
    async def subscribe(self, topics: List[str], subscription_name: str) -> None:
        """Subscribe to topics.

        Args:
            topics: List of topics to subscribe to
            subscription_name: Subscription name for consumer group
        """
        pass

    @abstractmethod
    async def consume_messages(self, timeout_seconds: float = 1.0) -> AsyncIterator[ConsumedMessage]:
        """Consume messages with timeout.

        Args:
            timeout_seconds: Timeout for polling messages

        Yields:
            ConsumedMessage instances
        """
        pass

    @abstractmethod
    async def commit(self) -> None:
        """Commit consumed message offsets."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close the consumer."""
        pass


class AbstractTopicManager(ABC):
    """Abstract topic management interface."""

    @abstractmethod
    async def create_topic(
        self,
        topic_name: str,
        partitions: int,
        replication_factor: int,
        config: Optional[Dict[str, str]] = None
    ) -> None:
        """Create a topic.

        Args:
            topic_name: Name of the topic
            partitions: Number of partitions
            replication_factor: Replication factor
            config: Topic configuration
        """
        pass

    @abstractmethod
    async def delete_topic(self, topic_name: str) -> None:
        """Delete a topic.

        Args:
            topic_name: Name of the topic to delete
        """
        pass

    @abstractmethod
    async def list_topics(self) -> List[str]:
        """List all topics.

        Returns:
            List of topic names
        """
        pass

    @abstractmethod
    async def topic_exists(self, topic_name: str) -> bool:
        """Check if topic exists.

        Args:
            topic_name: Name of the topic

        Returns:
            True if topic exists, False otherwise
        """
        pass


class AbstractDriver(LoggerMixin, ABC):
    """Abstract driver for messaging systems."""

    def __init__(self, driver_config: DriverConfig):
        super().__init__()
        self.config = driver_config
        self._initialized = False

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize the driver."""
        pass

    @abstractmethod
    async def cleanup(self) -> None:
        """Cleanup driver resources."""
        pass

    @abstractmethod
    def create_producer(self, config: Optional[Dict[str, Any]] = None) -> AbstractProducer:
        """Create a producer instance.

        Args:
            config: Producer-specific configuration

        Returns:
            Producer instance
        """
        pass

    @abstractmethod
    def create_consumer(self, config: Optional[Dict[str, Any]] = None) -> AbstractConsumer:
        """Create a consumer instance.

        Args:
            config: Consumer-specific configuration

        Returns:
            Consumer instance
        """
        pass

    @abstractmethod
    def create_topic_manager(self) -> AbstractTopicManager:
        """Create a topic manager instance.

        Returns:
            Topic manager instance
        """
        pass

    @property
    def driver_name(self) -> str:
        """Get driver name."""
        return self.config.name

    async def __aenter__(self):
        """Async context manager entry."""
        if not self._initialized:
            await self.initialize()
            self._initialized = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.cleanup()


class DriverUtils:
    """Utility functions for drivers."""

    @staticmethod
    def calculate_throughput_stats(
        total_messages: int,
        total_bytes: int,
        start_time: float,
        end_time: float
    ) -> ThroughputStats:
        """Calculate throughput statistics.

        Args:
            total_messages: Total number of messages
            total_bytes: Total bytes processed
            start_time: Start timestamp
            end_time: End timestamp

        Returns:
            ThroughputStats object
        """
        duration = max(end_time - start_time, 0.001)  # Avoid division by zero

        return ThroughputStats(
            total_messages=total_messages,
            total_bytes=total_bytes,
            duration_seconds=duration,
            messages_per_second=total_messages / duration,
            bytes_per_second=total_bytes / duration,
            mb_per_second=(total_bytes / (1024 * 1024)) / duration
        )

    @staticmethod
    def calculate_latency_stats(latencies_ms: List[float]) -> LatencyStats:
        """Calculate latency statistics.

        Args:
            latencies_ms: List of latencies in milliseconds

        Returns:
            LatencyStats object
        """
        if not latencies_ms:
            return LatencyStats()

        import numpy as np

        sorted_latencies = sorted(latencies_ms)
        count = len(sorted_latencies)

        return LatencyStats(
            count=count,
            min_ms=float(min(sorted_latencies)),
            max_ms=float(max(sorted_latencies)),
            mean_ms=float(np.mean(sorted_latencies)),
            median_ms=float(np.median(sorted_latencies)),
            p50_ms=float(np.percentile(sorted_latencies, 50)),
            p95_ms=float(np.percentile(sorted_latencies, 95)),
            p99_ms=float(np.percentile(sorted_latencies, 99)),
            p99_9_ms=float(np.percentile(sorted_latencies, 99.9))
        )

    @staticmethod
    def create_error_stats(errors: Dict[str, int]) -> ErrorStats:
        """Create error statistics.

        Args:
            errors: Dictionary of error types and counts

        Returns:
            ErrorStats object
        """
        total_errors = sum(errors.values())

        return ErrorStats(
            total_errors=total_errors,
            error_rate=0.0,  # Will be calculated later with total messages
            error_types=errors
        )

    @staticmethod
    def generate_message_key(pattern: str, message_index: int, total_messages: int) -> Optional[bytes]:
        """Generate message key based on pattern.

        Args:
            pattern: Key generation pattern
            message_index: Current message index
            total_messages: Total number of messages

        Returns:
            Generated key or None
        """
        if pattern == "NO_KEY":
            return None
        elif pattern == "RANDOM":
            import random
            return f"key-{random.randint(0, 10000)}".encode()
        elif pattern == "ROUND_ROBIN":
            return f"key-{message_index % 100}".encode()
        elif pattern == "ZIP_LATENT":
            # Zipfian distribution for key generation
            import random
            # Simple zipfian approximation
            key_space = min(1000, total_messages // 10)
            key_id = int(random.random() ** 2 * key_space)
            return f"key-{key_id}".encode()
        else:
            return f"key-{message_index}".encode()

    @staticmethod
    def create_test_payload(size_bytes: int, payload_data: Optional[bytes] = None) -> bytes:
        """Create test message payload.

        Args:
            size_bytes: Target size in bytes
            payload_data: Custom payload data

        Returns:
            Message payload
        """
        if payload_data:
            # Repeat or truncate payload data to match target size
            if len(payload_data) >= size_bytes:
                return payload_data[:size_bytes]
            else:
                # Repeat payload to reach target size
                repetitions = (size_bytes // len(payload_data)) + 1
                repeated = payload_data * repetitions
                return repeated[:size_bytes]
        else:
            # Generate default payload
            return b'x' * size_bytes