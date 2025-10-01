"""Kafka topic management implementation."""

import asyncio
from typing import Dict, List, Any, Optional
from ..base import AbstractTopicManager
from ...core.config import DriverConfig
from ...utils.logging import LoggerMixin


class KafkaTopicManager(AbstractTopicManager, LoggerMixin):
    """Kafka topic manager supporting multiple Python clients."""

    def __init__(self, driver_config: DriverConfig, client_type: str):
        """Initialize Kafka topic manager.

        Args:
            driver_config: Driver configuration
            client_type: Kafka client type
        """
        super().__init__()
        self.config = driver_config
        self.client_type = client_type
        self._admin_client = None

        # Get bootstrap servers
        self.bootstrap_servers = self.config.common_config.get("bootstrap.servers", "localhost:9092")

    async def _initialize_admin_client(self):
        """Initialize the appropriate Kafka admin client."""
        if self._admin_client is not None:
            return

        try:
            if self.client_type == "kafka-python":
                await self._init_kafka_python_admin()
            elif self.client_type == "confluent-kafka":
                await self._init_confluent_kafka_admin()
            elif self.client_type == "aiokafka":
                await self._init_aiokafka_admin()
            else:
                raise ValueError(f"Unsupported client type: {self.client_type}")

            self.logger.info(f"Initialized {self.client_type} admin client")

        except Exception as e:
            self.logger.error(f"Failed to initialize {self.client_type} admin client: {e}")
            raise

    async def _init_kafka_python_admin(self):
        """Initialize kafka-python admin client."""
        try:
            from kafka.admin import KafkaAdminClient
        except ImportError:
            raise ImportError("kafka-python not available. Install with: pip install kafka-python")

        config = {
            "bootstrap_servers": self.bootstrap_servers.split(','),
            "request_timeout_ms": 30000,
            "connections_max_idle_ms": 300000,
        }

        # Add common config
        for key, value in self.config.common_config.items():
            if key == "bootstrap.servers":
                continue  # Already handled
            elif key == "request.timeout.ms":
                config["request_timeout_ms"] = int(value)
            elif key == "connections.max.idle.ms":
                config["connections_max_idle_ms"] = int(value)

        self._admin_client = KafkaAdminClient(**config)

    async def _init_confluent_kafka_admin(self):
        """Initialize confluent-kafka admin client."""
        try:
            from confluent_kafka.admin import AdminClient
        except ImportError:
            raise ImportError("confluent-kafka not available. Install with: pip install confluent-kafka")

        # AdminClient only supports a limited set of parameters
        # Filter out parameters that don't apply to admin operations
        admin_config = {
            "bootstrap.servers": self.config.common_config.get("bootstrap.servers", self.bootstrap_servers)
        }

        # Only add parameters that AdminClient actually supports
        supported_params = {
            'socket.timeout.ms', 'socket.keepalive.enable',
            'socket.send.buffer.bytes', 'socket.receive.buffer.bytes',
            'connections.max.idle.ms', 'reconnect.backoff.ms',
            'reconnect.backoff.max.ms', 'request.timeout.ms',
            'metadata.max.age.ms', 'sasl.mechanism', 'sasl.username',
            'sasl.password', 'ssl.ca.location', 'ssl.certificate.location',
            'ssl.key.location', 'security.protocol'
        }

        for key, value in self.config.common_config.items():
            if key in supported_params or key.startswith('sasl.') or key.startswith('ssl.'):
                admin_config[key] = value

        self._admin_client = AdminClient(admin_config)

    async def _init_aiokafka_admin(self):
        """Initialize aiokafka admin client."""
        try:
            from aiokafka.admin import AIOKafkaAdminClient
        except ImportError:
            raise ImportError("aiokafka not available. Install with: pip install aiokafka")

        self._admin_client = AIOKafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers
        )
        await self._admin_client.start()

    async def create_topic(
        self,
        topic_name: str,
        partitions: int,
        replication_factor: int,
        config: Optional[Dict[str, str]] = None
    ) -> None:
        """Create a topic."""
        if self._admin_client is None:
            await self._initialize_admin_client()

        try:
            if self.client_type == "kafka-python":
                await self._create_topic_kafka_python(topic_name, partitions, replication_factor, config)
            elif self.client_type == "confluent-kafka":
                await self._create_topic_confluent_kafka(topic_name, partitions, replication_factor, config)
            elif self.client_type == "aiokafka":
                await self._create_topic_aiokafka(topic_name, partitions, replication_factor, config)

            self.logger.info(f"Created topic: {topic_name} with {partitions} partitions")

        except Exception as e:
            # Don't treat "topic already exists" as an error
            if "already exists" in str(e).lower() or "error_code=36" in str(e) or "TopicAlreadyExistsError" in str(e):
                self.logger.info(f"Topic {topic_name} already exists, skipping creation")
            else:
                self.logger.error(f"Failed to create topic {topic_name}: {e}")
                raise

    async def _create_topic_kafka_python(
        self,
        topic_name: str,
        partitions: int,
        replication_factor: int,
        config: Optional[Dict[str, str]]
    ):
        """Create topic using kafka-python."""
        from kafka.admin import NewTopic

        topic_config = {}
        if config:
            topic_config.update(config)

        # Add driver topic config
        topic_config.update(self.config.topic_config)

        new_topic = NewTopic(
            name=topic_name,
            num_partitions=partitions,
            replication_factor=replication_factor,
            topic_configs=topic_config
        )

        def blocking_create():
            result = self._admin_client.create_topics([new_topic])
            # Wait for the result
            for topic, future in result.items():
                try:
                    future.result(timeout=30)  # 30 seconds timeout
                except Exception as e:
                    if "already exists" in str(e).lower() or "error_code=36" in str(e):
                        self.logger.info(f"Topic {topic} already exists, skipping creation")
                    else:
                        raise

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, blocking_create)

    async def _create_topic_confluent_kafka(
        self,
        topic_name: str,
        partitions: int,
        replication_factor: int,
        config: Optional[Dict[str, str]]
    ):
        """Create topic using confluent-kafka."""
        from confluent_kafka.admin import NewTopic

        topic_config = {}
        if config:
            topic_config.update(config)

        # Add driver topic config
        topic_config.update(self.config.topic_config)

        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=partitions,
            replication_factor=replication_factor,
            config=topic_config
        )

        def blocking_create():
            result = self._admin_client.create_topics([new_topic])
            # Wait for the result
            for topic, future in result.items():
                try:
                    future.result(timeout=30)  # 30 seconds timeout
                except Exception as e:
                    if "already exists" in str(e).lower() or "error_code=36" in str(e):
                        self.logger.info(f"Topic {topic} already exists, skipping creation")
                    else:
                        raise

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, blocking_create)

    async def _create_topic_aiokafka(
        self,
        topic_name: str,
        partitions: int,
        replication_factor: int,
        config: Optional[Dict[str, str]]
    ):
        """Create topic using aiokafka."""
        from aiokafka.admin.new_topic import NewTopic

        topic_config = {}
        if config:
            topic_config.update(config)

        # Add driver topic config
        topic_config.update(self.config.topic_config)

        new_topic = NewTopic(
            name=topic_name,
            num_partitions=partitions,
            replication_factor=replication_factor,
            topic_configs=topic_config
        )

        try:
            await self._admin_client.create_topics([new_topic])
        except Exception as e:
            if "already exists" in str(e).lower():
                self.logger.info(f"Topic {topic_name} already exists")
            else:
                raise

    async def delete_topic(self, topic_name: str) -> None:
        """Delete a topic."""
        if self._admin_client is None:
            await self._initialize_admin_client()

        try:
            if self.client_type == "kafka-python":
                def blocking_delete():
                    result = self._admin_client.delete_topics([topic_name])
                    # Handle both dict and response object formats
                    if hasattr(result, 'items'):
                        for topic, future in result.items():
                            future.result(timeout=30)
                    else:
                        # For newer kafka-python versions, result is response object
                        import time
                        time.sleep(1)  # Simple wait for deletion

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, blocking_delete)

            elif self.client_type == "confluent-kafka":
                def blocking_delete():
                    result = self._admin_client.delete_topics([topic_name])
                    for topic, future in result.items():
                        future.result(timeout=30)

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, blocking_delete)

            elif self.client_type == "aiokafka":
                await self._admin_client.delete_topics([topic_name])

            self.logger.info(f"Deleted topic: {topic_name}")

        except Exception as e:
            # Don't treat "topic doesn't exist" as an error
            if "error_code=3" in str(e) or "UnknownTopicOrPartitionError" in str(e):
                self.logger.info(f"Topic {topic_name} doesn't exist, skipping deletion")
            else:
                self.logger.error(f"Failed to delete topic {topic_name}: {e}")
                raise

    async def list_topics(self) -> List[str]:
        """List all topics."""
        if self._admin_client is None:
            await self._initialize_admin_client()

        try:
            if self.client_type == "kafka-python":
                def blocking_list():
                    metadata = self._admin_client.describe_cluster()
                    if hasattr(metadata, 'topics'):
                        return list(metadata.topics.keys())
                    else:
                        # metadata is a dict in kafka-python
                        return list(metadata.keys()) if isinstance(metadata, dict) else []

                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, blocking_list)

            elif self.client_type == "confluent-kafka":
                def blocking_list():
                    metadata = self._admin_client.list_topics(timeout=10)
                    return list(metadata.topics.keys())

                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, blocking_list)

            elif self.client_type == "aiokafka":
                metadata = await self._admin_client.describe_cluster()
                return list(metadata.topics.keys())

        except Exception as e:
            self.logger.error(f"Failed to list topics: {e}")
            raise

    async def topic_exists(self, topic_name: str) -> bool:
        """Check if topic exists."""
        try:
            topics = await self.list_topics()
            return topic_name in topics
        except Exception as e:
            self.logger.error(f"Failed to check if topic exists: {e}")
            return False

    async def close(self) -> None:
        """Close the admin client."""
        if self._admin_client is None:
            return

        try:
            if self.client_type == "kafka-python":
                def blocking_close():
                    self._admin_client.close()

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, blocking_close)

            elif self.client_type == "confluent-kafka":
                # Confluent Kafka admin client doesn't have explicit close
                pass

            elif self.client_type == "aiokafka":
                await self._admin_client.close()

            self._admin_client = None
            self.logger.info(f"Closed {self.client_type} admin client")

        except Exception as e:
            self.logger.error(f"Failed to close admin client: {e}")
            raise