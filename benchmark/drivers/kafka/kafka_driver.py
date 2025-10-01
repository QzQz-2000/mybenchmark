"""Kafka driver implementation."""

import asyncio
from typing import Dict, Any, Optional
from ..base import AbstractDriver
from .kafka_producer import KafkaProducer
from .kafka_consumer import KafkaConsumer
from .kafka_topic_manager import KafkaTopicManager
from ...core.config import DriverConfig


class KafkaDriver(AbstractDriver):
    """Kafka driver implementation supporting multiple Python Kafka clients."""

    def __init__(self, driver_config: DriverConfig):
        """Initialize Kafka driver with Confluent Kafka client.

        Args:
            driver_config: Driver configuration
        """
        super().__init__(driver_config)
        self.client_type = "confluent-kafka"
        self._topic_manager: Optional[KafkaTopicManager] = None

        self.logger.info("Initialized Kafka driver with Confluent client for Digital Twin scenarios")

    async def initialize(self) -> None:
        """Initialize the Kafka driver."""
        self.logger.info("Initializing Kafka driver")

        # Create topic manager
        self._topic_manager = KafkaTopicManager(self.config, self.client_type)

        # Create topics if needed and reset is enabled
        if self.config.reset:
            await self._setup_topics()

        self.logger.info("Kafka driver initialized successfully")

    async def cleanup(self) -> None:
        """Cleanup Kafka driver resources."""
        self.logger.info("Cleaning up Kafka driver")

        if self._topic_manager:
            await self._topic_manager.close()

        self.logger.info("Kafka driver cleanup completed")

    def create_producer(self, config: Optional[Dict[str, Any]] = None) -> KafkaProducer:
        """Create a Kafka producer instance.

        Args:
            config: Producer-specific configuration override

        Returns:
            KafkaProducer instance
        """
        producer_config = self.config.producer_config.copy()
        if config:
            producer_config.update(config)

        return KafkaProducer(
            bootstrap_servers=self._get_bootstrap_servers(),
            config=producer_config,
            common_config=self.config.common_config
        )

    def create_consumer(self, config: Optional[Dict[str, Any]] = None) -> KafkaConsumer:
        """Create a Kafka consumer instance.

        Args:
            config: Consumer-specific configuration override

        Returns:
            KafkaConsumer instance
        """
        print(f"[KAFKA DRIVER] self.config.consumer_config = {self.config.consumer_config}", flush=True)
        consumer_config = self.config.consumer_config.copy()
        if config:
            consumer_config.update(config)
        print(f"[KAFKA DRIVER] Creating consumer with config: {consumer_config}", flush=True)

        return KafkaConsumer(
            bootstrap_servers=self._get_bootstrap_servers(),
            config=consumer_config,
            common_config=self.config.common_config
        )

    def create_topic_manager(self) -> KafkaTopicManager:
        """Create a Kafka topic manager instance.

        Returns:
            KafkaTopicManager instance
        """
        return KafkaTopicManager(self.config, self.client_type)

    async def _setup_topics(self) -> None:
        """Setup topics based on configuration."""
        # This would be called by the coordinator based on workload configuration
        # For now, we'll just ensure the topic manager is ready
        pass

    def _get_bootstrap_servers(self) -> str:
        """Get bootstrap servers from configuration."""
        return self.config.common_config.get("bootstrap.servers", "localhost:9092")

    def get_client_info(self) -> Dict[str, Any]:
        """Get client information."""
        return {
            "driver_name": self.driver_name,
            "client_type": "confluent-kafka",
            "bootstrap_servers": self._get_bootstrap_servers(),
            "replication_factor": self.config.replication_factor,
            "optimized_for": "digital_twin_scenarios"
        }