"""Configuration validation utilities for benchmark framework.

Validates driver and workload configurations to catch issues early.
"""

from typing import Dict, List, Set, Optional
import re


class ConfigValidationError(Exception):
    """Configuration validation error."""
    pass


class KafkaConfigValidator:
    """Validator for Kafka driver configurations."""

    # Valid Confluent Kafka producer parameters
    VALID_PRODUCER_PARAMS: Set[str] = {
        'bootstrap.servers', 'client.id', 'acks', 'retries', 'batch.size',
        'linger.ms', 'buffer.memory', 'compression.type', 'max.in.flight.requests.per.connection',
        'enable.idempotence', 'transactional.id', 'transaction.timeout.ms',
        'delivery.timeout.ms', 'request.timeout.ms', 'max.request.size',
        'partitioner', 'compression.level', 'socket.keepalive.enable',
        'socket.send.buffer.bytes', 'socket.receive.buffer.bytes',
        'connections.max.idle.ms', 'retry.backoff.ms', 'reconnect.backoff.ms',
        'reconnect.backoff.max.ms', 'message.max.bytes', 'queue.buffering.max.messages',
        'queue.buffering.max.kbytes', 'message.send.max.retries', 'default.api.timeout.ms'
    }

    # Valid Confluent Kafka consumer parameters
    VALID_CONSUMER_PARAMS: Set[str] = {
        'bootstrap.servers', 'group.id', 'client.id', 'enable.auto.commit',
        'auto.commit.interval.ms', 'auto.offset.reset', 'fetch.min.bytes',
        'fetch.max.bytes', 'fetch.max.wait.ms', 'max.partition.fetch.bytes',
        'session.timeout.ms', 'heartbeat.interval.ms', 'max.poll.interval.ms',
        'partition.assignment.strategy', 'connections.max.idle.ms',
        'default.api.timeout.ms', 'request.timeout.ms', 'isolation.level',
        'socket.keepalive.enable', 'socket.send.buffer.bytes',
        'socket.receive.buffer.bytes', 'reconnect.backoff.ms',
        'reconnect.backoff.max.ms', 'check.crcs'
    }

    # Invalid parameters that don't work with Confluent Kafka Python client
    INVALID_PARAMS: Set[str] = {
        'max.poll.records',  # Java client only
        'key.serializer',  # Java client only
        'value.serializer',  # Java client only
        'key.deserializer',  # Java client only
        'value.deserializer',  # Java client only
    }

    # Deprecated or problematic settings
    DEPRECATED_PARAMS: Dict[str, str] = {
        'auto.offset.reset=earliest': 'Consider using "latest" for fresh benchmark tests',
        'enable.auto.commit=true': 'Should be false for accurate benchmark measurement',
    }

    @classmethod
    def validate_producer_config(cls, config: Dict[str, str]) -> List[str]:
        """Validate producer configuration.

        Args:
            config: Producer configuration dictionary

        Returns:
            List of warning messages (empty if no issues)
        """
        warnings = []

        for key, value in config.items():
            # Check for invalid parameters
            if key in cls.INVALID_PARAMS:
                warnings.append(
                    f"⚠️  Invalid producer parameter '{key}': "
                    f"Not supported by Confluent Kafka Python client"
                )

            # Check for unknown parameters (might be typos)
            elif key not in cls.VALID_PRODUCER_PARAMS and not key.startswith('sasl.') and not key.startswith('ssl.'):
                warnings.append(
                    f"⚠️  Unknown producer parameter '{key}': "
                    f"May not be supported, check documentation"
                )

            # Check deprecated settings
            setting = f"{key}={value}"
            if setting in cls.DEPRECATED_PARAMS:
                warnings.append(
                    f"⚠️  {setting}: {cls.DEPRECATED_PARAMS[setting]}"
                )

        # Check required parameters
        if 'bootstrap.servers' not in config:
            warnings.append("⚠️  Missing 'bootstrap.servers' in producer config")

        return warnings

    @classmethod
    def validate_consumer_config(cls, config: Dict[str, str]) -> List[str]:
        """Validate consumer configuration.

        Args:
            config: Consumer configuration dictionary

        Returns:
            List of warning messages (empty if no issues)
        """
        warnings = []

        for key, value in config.items():
            # Check for invalid parameters
            if key in cls.INVALID_PARAMS:
                warnings.append(
                    f"⚠️  Invalid consumer parameter '{key}': "
                    f"Not supported by Confluent Kafka Python client"
                )

            # Check for unknown parameters
            elif key not in cls.VALID_CONSUMER_PARAMS and not key.startswith('sasl.') and not key.startswith('ssl.'):
                warnings.append(
                    f"⚠️  Unknown consumer parameter '{key}': "
                    f"May not be supported, check documentation"
                )

            # Check deprecated settings
            setting = f"{key}={value}"
            if setting in cls.DEPRECATED_PARAMS:
                warnings.append(
                    f"⚠️  {setting}: {cls.DEPRECATED_PARAMS[setting]}"
                )

        # Check required parameters
        if 'bootstrap.servers' not in config:
            warnings.append("⚠️  Missing 'bootstrap.servers' in consumer config")

        # Warn about problematic offset reset for benchmarks
        if config.get('auto.offset.reset') == 'earliest':
            warnings.append(
                "⚠️  auto.offset.reset=earliest: May re-consume old data in benchmarks. "
                "Consider 'latest' for fresh tests with unique topics"
            )

        return warnings

    @classmethod
    def validate_common_config(cls, config: Dict[str, str]) -> List[str]:
        """Validate common configuration.

        Args:
            config: Common configuration dictionary

        Returns:
            List of warning messages (empty if no issues)
        """
        warnings = []

        if 'bootstrap.servers' not in config:
            warnings.append("⚠️  Missing 'bootstrap.servers' in common config")
        else:
            servers = config['bootstrap.servers']
            # Basic validation of server format
            if not re.match(r'^[\w\.\-]+(:\d+)?(,[\w\.\-]+(:\d+)?)*$', servers):
                warnings.append(
                    f"⚠️  Invalid bootstrap.servers format: {servers}. "
                    f"Expected format: host1:port1,host2:port2"
                )

        return warnings


class WorkloadConfigValidator:
    """Validator for workload configurations."""

    @classmethod
    def validate_workload(cls, config: Dict) -> List[str]:
        """Validate workload configuration.

        Args:
            config: Workload configuration dictionary

        Returns:
            List of warning messages (empty if no issues)
        """
        warnings = []

        # Check required fields (support both camelCase and snake_case)
        required_fields = {
            'name': ['name'],
            'topics': ['topics'],
            'partitionsPerTopic': ['partitionsPerTopic', 'partitions_per_topic'],
            'messageSize': ['messageSize', 'message_size']
        }

        for field_name, field_variants in required_fields.items():
            if not any(variant in config for variant in field_variants):
                warnings.append(f"⚠️  Missing required field '{field_name}' in workload config")

        # Validate numeric ranges
        if 'producerRate' in config and config['producerRate'] < 0:
            warnings.append("⚠️  producerRate cannot be negative")

        if 'testDurationMinutes' in config and config['testDurationMinutes'] < 1:
            warnings.append("⚠️  testDurationMinutes should be at least 1")

        # Check for unrealistic values
        if 'producerRate' in config and config['producerRate'] > 10_000_000:
            warnings.append(
                f"⚠️  Very high producerRate ({config['producerRate']} msg/s): "
                f"May not be achievable"
            )

        if 'messageSize' in config and config['messageSize'] > 10_000_000:
            warnings.append(
                f"⚠️  Very large messageSize ({config['messageSize']} bytes): "
                f"May cause memory issues"
            )

        # Check for backlog testing
        if 'consumerBacklogSizeGB' in config and config['consumerBacklogSizeGB'] > 0:
            warnings.append(
                "⚠️  consumerBacklogSizeGB > 0: Backlog testing not fully implemented yet"
            )

        return warnings


def validate_driver_config(driver_config) -> List[str]:
    """Validate driver configuration.

    Args:
        driver_config: DriverConfig object

    Returns:
        List of warning messages
    """
    warnings = []

    # Validate based on driver type
    if 'kafka' in driver_config.driver_class.lower():
        validator = KafkaConfigValidator()

        # Validate common config first
        common_warnings = validator.validate_common_config(driver_config.common_config)
        warnings.extend(common_warnings)

        # If bootstrap.servers is in common_config, don't warn about it in producer/consumer
        has_bootstrap_servers = 'bootstrap.servers' in driver_config.common_config

        # Validate producer config
        producer_warnings = validator.validate_producer_config(driver_config.producer_config)
        # Filter out bootstrap.servers warning if it's in common_config
        if has_bootstrap_servers:
            producer_warnings = [w for w in producer_warnings if 'bootstrap.servers' not in w]
        warnings.extend(producer_warnings)

        # Validate consumer config
        consumer_warnings = validator.validate_consumer_config(driver_config.consumer_config)
        # Filter out bootstrap.servers warning if it's in common_config
        if has_bootstrap_servers:
            consumer_warnings = [w for w in consumer_warnings if 'bootstrap.servers' not in w]
        warnings.extend(consumer_warnings)

    return warnings


def validate_workload_config(workload_config) -> List[str]:
    """Validate workload configuration.

    Args:
        workload_config: WorkloadConfig object or dict

    Returns:
        List of warning messages
    """
    if hasattr(workload_config, 'dict'):
        config_dict = workload_config.dict()
    else:
        config_dict = workload_config

    return WorkloadConfigValidator.validate_workload(config_dict)


def print_validation_warnings(warnings: List[str], logger=None):
    """Print validation warnings.

    Args:
        warnings: List of warning messages
        logger: Optional logger to use
    """
    if not warnings:
        return

    message = "\n" + "="*60 + "\n"
    message += "⚠️  CONFIGURATION VALIDATION WARNINGS\n"
    message += "="*60 + "\n"

    for warning in warnings:
        message += f"{warning}\n"

    message += "="*60 + "\n"

    if logger:
        logger.warning(message)
    else:
        print(message)