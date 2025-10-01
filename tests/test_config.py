"""Test configuration management."""

import pytest
import tempfile
import yaml
from pathlib import Path

from benchmark.core.config import (
    WorkloadConfig,
    DriverConfig,
    BenchmarkConfig,
    ConfigLoader,
    KeyDistributor
)


class TestWorkloadConfig:
    """Test workload configuration."""

    def test_workload_config_basic(self):
        """Test basic workload configuration."""
        config = WorkloadConfig(
            name="Test Workload",
            topics=2,
            partitionsPerTopic=4,
            messageSize=1024,
            producersPerTopic=2,
            producerRate=10000,
            testDurationMinutes=5
        )

        assert config.name == "Test Workload"
        assert config.topics == 2
        assert config.partitions_per_topic == 4
        assert config.message_size == 1024
        assert config.producers_per_topic == 2
        assert config.producer_rate == 10000
        assert config.test_duration_minutes == 5
        assert config.key_distributor == KeyDistributor.NO_KEY

    def test_workload_config_from_dict(self):
        """Test workload configuration from dictionary."""
        data = {
            "name": "Test Workload",
            "topics": 1,
            "partitionsPerTopic": 8,
            "keyDistributor": "ROUND_ROBIN",
            "messageSize": 2048,
            "producersPerTopic": 4,
            "producerRate": 50000,
            "consumerPerSubscription": 2,
            "testDurationMinutes": 10
        }

        config = WorkloadConfig(**data)
        assert config.name == "Test Workload"
        assert config.partitions_per_topic == 8
        assert config.key_distributor == KeyDistributor.ROUND_ROBIN
        assert config.message_size == 2048

    def test_workload_config_validation(self):
        """Test workload configuration validation."""
        # Test invalid topics count
        with pytest.raises(ValueError):
            WorkloadConfig(
                name="Invalid",
                topics=0,  # Should be >= 1
                testDurationMinutes=5
            )

        # Test invalid message size
        with pytest.raises(ValueError):
            WorkloadConfig(
                name="Invalid",
                messageSize=0,  # Should be >= 1
                testDurationMinutes=5
            )


class TestDriverConfig:
    """Test driver configuration."""

    def test_driver_config_basic(self):
        """Test basic driver configuration."""
        config = DriverConfig(
            name="Kafka",
            driverClass="benchmark.drivers.kafka.KafkaDriver"
        )

        assert config.name == "Kafka"
        assert config.driver_class == "benchmark.drivers.kafka.KafkaDriver"
        assert config.replication_factor == 1
        assert isinstance(config.common_config, dict)

    def test_driver_config_with_string_configs(self):
        """Test driver configuration with string configs."""
        data = {
            "name": "Kafka",
            "driverClass": "benchmark.drivers.kafka.KafkaDriver",
            "commonConfig": "bootstrap.servers=localhost:9092\nacks=all",
            "producerConfig": "batch.size=16384\nlinger.ms=10",
            "consumerConfig": "auto.offset.reset=earliest\nenable.auto.commit=false"
        }

        config = DriverConfig(**data)
        assert config.common_config["bootstrap.servers"] == "localhost:9092"
        assert config.common_config["acks"] == "all"
        assert config.producer_config["batch.size"] == "16384"
        assert config.producer_config["linger.ms"] == "10"
        assert config.consumer_config["auto.offset.reset"] == "earliest"

    def test_driver_config_with_dict_configs(self):
        """Test driver configuration with dictionary configs."""
        data = {
            "name": "Kafka",
            "driverClass": "benchmark.drivers.kafka.KafkaDriver",
            "commonConfig": {
                "bootstrap.servers": "localhost:9092",
                "acks": "all"
            },
            "producerConfig": {
                "batch.size": "16384"
            }
        }

        config = DriverConfig(**data)
        assert config.common_config["bootstrap.servers"] == "localhost:9092"
        assert config.producer_config["batch.size"] == "16384"


class TestConfigLoader:
    """Test configuration loader."""

    def test_load_workload_config(self):
        """Test loading workload configuration from file."""
        workload_data = {
            "name": "Test Workload",
            "topics": 1,
            "partitionsPerTopic": 4,
            "messageSize": 1024,
            "producersPerTopic": 2,
            "producerRate": 10000,
            "testDurationMinutes": 5
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(workload_data, f)
            temp_path = f.name

        try:
            config = ConfigLoader.load_workload(temp_path)
            assert config.name == "Test Workload"
            assert config.topics == 1
            assert config.partitions_per_topic == 4
        finally:
            Path(temp_path).unlink()

    def test_load_driver_config(self):
        """Test loading driver configuration from file."""
        driver_data = {
            "name": "Kafka",
            "driverClass": "benchmark.drivers.kafka.KafkaDriver",
            "replicationFactor": 3,
            "commonConfig": "bootstrap.servers=localhost:9092"
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(driver_data, f)
            temp_path = f.name

        try:
            config = ConfigLoader.load_driver(temp_path)
            assert config.name == "Kafka"
            assert config.replication_factor == 3
            assert config.common_config["bootstrap.servers"] == "localhost:9092"
        finally:
            Path(temp_path).unlink()

    def test_save_config(self):
        """Test saving configuration to file."""
        config = WorkloadConfig(
            name="Test Save",
            topics=1,
            partitionsPerTopic=2,
            messageSize=512,
            testDurationMinutes=5
        )

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            temp_path = f.name

        try:
            ConfigLoader.save_config(config, temp_path)

            # Load it back
            loaded_config = ConfigLoader.load_workload(temp_path)
            assert loaded_config.name == config.name
            assert loaded_config.topics == config.topics
        finally:
            Path(temp_path).unlink()


class TestBenchmarkConfig:
    """Test benchmark configuration."""

    def test_benchmark_config_defaults(self):
        """Test benchmark configuration defaults."""
        config = BenchmarkConfig()

        assert config.workers == []
        assert config.log_level == "INFO"
        assert config.results_dir == "results"
        assert config.enable_monitoring is True

    def test_benchmark_config_custom(self):
        """Test benchmark configuration with custom values."""
        config = BenchmarkConfig(
            workers=["http://localhost:8080", "http://localhost:8081"],
            log_level="DEBUG",
            results_dir="custom_results",
            enable_monitoring=False
        )

        assert len(config.workers) == 2
        assert config.log_level == "DEBUG"
        assert config.results_dir == "custom_results"
        assert config.enable_monitoring is False