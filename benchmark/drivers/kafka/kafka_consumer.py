"""Confluent Kafka consumer implementation for performance testing."""

import time
import asyncio
from typing import Dict, List, Any, Optional, AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from ..base import AbstractConsumer, Message, ConsumedMessage
from ...utils.logging import LoggerMixin


class KafkaConsumer(AbstractConsumer, LoggerMixin):
    """Confluent Kafka consumer optimized for performance testing."""

    def __init__(
        self,
        bootstrap_servers: str,
        config: Dict[str, Any],
        common_config: Optional[Dict[str, Any]] = None
    ):
        """Initialize Confluent Kafka consumer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            config: Consumer configuration
            common_config: Common configuration
        """
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.config = config
        self.common_config = common_config or {}
        self._consumer = None
        self._subscribed_topics = []
        self._running = False
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="kafka-consumer")

        # Merge configurations for performance testing
        self._merged_config = self._merge_configs()
        self.logger.debug(f"Consumer initialized with auto.offset.reset={self._merged_config.get('auto.offset.reset', 'NOT_SET')}")

    def _merge_configs(self) -> Dict[str, Any]:
        """Merge common and consumer-specific configurations."""
        # Filter out admin-only configurations and Java-style configs that Consumer doesn't support
        # librdkafka uses different config names than Java Kafka
        consumer_incompatible_keys = {
            'default.api.timeout.ms',  # Admin API only
            'buffer.memory',            # Java producer config (not applicable to consumer)
        }

        merged = {k: v for k, v in self.common_config.items() if k not in consumer_incompatible_keys}
        # Also filter from consumer-specific config
        merged.update({k: v for k, v in self.config.items() if k not in consumer_incompatible_keys})

        # Ensure bootstrap servers is set
        if "bootstrap.servers" not in merged:
            merged["bootstrap.servers"] = self.bootstrap_servers

        # Convert all config values to strings for confluent-kafka compatibility
        merged = {k: str(v) if not isinstance(v, str) else v for k, v in merged.items()}

        # Performance testing defaults - only valid Confluent Kafka parameters
        # NOTE: Using "latest" offset reset for unique topics to avoid re-consuming old data
        perf_defaults = {
            "enable.auto.commit": "false",  # Manual commit for accurate testing
            "auto.offset.reset": "latest",  # Start from latest for fresh benchmarks
            "session.timeout.ms": "30000",
            "heartbeat.interval.ms": "3000",
            "max.poll.interval.ms": "300000",  # 5 minutes max poll interval
            # Confluent Kafka specific optimizations
            "fetch.min.bytes": "1",  # Fetch immediately when data available
            "fetch.max.bytes": "52428800",  # 50MB max fetch
            "max.partition.fetch.bytes": "1048576",  # 1MB per partition
        }

        # Apply defaults only if not explicitly set
        for key, value in perf_defaults.items():
            if key not in merged:
                merged[key] = value

        return merged

    async def _initialize_consumer(self):
        """Initialize the Confluent Kafka consumer."""
        if self._consumer is not None:
            return

        try:
            from confluent_kafka import Consumer
        except ImportError:
            raise ImportError("confluent-kafka not available. Install with: pip install confluent-kafka")

        self._consumer = Consumer(self._merged_config)
        self.logger.info("Initialized Confluent Kafka consumer for performance testing")

    async def subscribe(self, topics: List[str], subscription_name: str) -> None:
        """Subscribe to topics with consumer group."""
        if self._consumer is None:
            # Add group.id for consumer group
            self._merged_config["group.id"] = subscription_name
            await self._initialize_consumer()

        self._subscribed_topics = topics

        def blocking_subscribe():
            self._consumer.subscribe(topics)
            # Don't wait for assignment - let it happen naturally during consume

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self._executor, blocking_subscribe)

        self.logger.info(f"Subscribed to topics {topics} with group {subscription_name}")

    async def consume_messages(self, timeout_seconds: float = 1.0) -> AsyncIterator[ConsumedMessage]:
        """Consume messages with timeout - optimized for performance testing."""
        if self._consumer is None:
            return

        self._running = True

        def blocking_poll():
            """Blocking poll operation."""
            messages = []
            end_time = time.time() + timeout_seconds

            while time.time() < end_time and self._running:
                # Poll with short timeout for responsiveness
                msg = self._consumer.poll(0.1)

                if msg is None:
                    continue

                if msg.error():
                    self.logger.error(f"Consumer error: {msg.error()}")
                    continue

                # Convert to our message format
                consumed_msg = ConsumedMessage(
                    message=Message(
                        key=msg.key(),
                        value=msg.value(),
                        headers=dict(msg.headers()) if msg.headers() else {},
                        timestamp=msg.timestamp()[1] / 1000 if msg.timestamp()[1] else time.time()
                    ),
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    receive_time=time.time()
                )
                messages.append(consumed_msg)

                # Commit periodically for performance testing
                if len(messages) % 100 == 0:
                    try:
                        self._consumer.commit(msg)
                    except Exception as e:
                        self.logger.warning(f"Commit failed: {e}")

            return messages

        # Execute polling in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        messages = await loop.run_in_executor(self._executor, blocking_poll)

        # Yield all consumed messages
        for msg in messages:
            yield msg

    async def commit_offsets(self) -> None:
        """Commit current offsets."""
        if self._consumer is None:
            return

        def blocking_commit():
            try:
                self._consumer.commit()
            except Exception as e:
                self.logger.error(f"Failed to commit offsets: {e}")
                raise

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self._executor, blocking_commit)

    async def commit(self) -> None:
        """Commit current offsets - alias for commit_offsets."""
        await self.commit_offsets()

    async def close(self) -> None:
        """Close the consumer."""
        if self._consumer is None:
            return

        self._running = False

        try:
            # Final commit
            await self.commit_offsets()

            def blocking_close():
                self._consumer.close()

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(self._executor, blocking_close)

            # Shutdown executor
            self._executor.shutdown(wait=True)

            self._consumer = None
            self.logger.info("Closed Confluent Kafka consumer")

        except Exception as e:
            self.logger.error(f"Failed to close consumer: {e}")
            raise

    def get_metrics(self) -> Dict[str, Any]:
        """Get consumer metrics for performance analysis."""
        if self._consumer is None:
            return {}

        return {
            "subscribed_topics": self._subscribed_topics,
            "client_type": "confluent-kafka",
            "running": self._running,
            "consumer_group": self._merged_config.get("group.id", "unknown")
        }