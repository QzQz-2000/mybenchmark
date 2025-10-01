"""Configuration management for the benchmark framework."""

import os
import yaml
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from pydantic import BaseModel, Field, validator
from enum import Enum


class KeyDistributor(str, Enum):
    """Key distribution strategies."""
    NO_KEY = "NO_KEY"
    RANDOM = "RANDOM"
    ROUND_ROBIN = "ROUND_ROBIN"
    ZIP_LATENT = "ZIP_LATENT"


class WorkloadConfig(BaseModel):
    """Workload configuration model."""

    name: str = Field(..., description="Workload name")
    topics: int = Field(1, ge=1, description="Number of topics")
    partitions_per_topic: int = Field(alias="partitionsPerTopic", default=1, ge=1)
    key_distributor: KeyDistributor = Field(alias="keyDistributor", default=KeyDistributor.NO_KEY)
    message_size: int = Field(alias="messageSize", default=1024, ge=1)
    payload_file: Optional[str] = Field(alias="payloadFile", default=None)

    # Producer settings
    producers_per_topic: int = Field(alias="producersPerTopic", default=1, ge=1)
    producer_rate: int = Field(alias="producerRate", default=10000, ge=0)

    # Consumer settings
    subscriptions_per_topic: int = Field(alias="subscriptionsPerTopic", default=1, ge=1)
    consumer_per_subscription: int = Field(alias="consumerPerSubscription", default=1, ge=1)
    consumer_backlog_size_gb: int = Field(alias="consumerBacklogSizeGB", default=0, ge=0)

    # Test duration
    test_duration_minutes: int = Field(alias="testDurationMinutes", default=15, ge=1)
    warmup_duration_minutes: int = Field(alias="warmupDurationMinutes", default=1, ge=0)

    class Config:
        populate_by_name = True
        use_enum_values = True


class DriverConfig(BaseModel):
    """Driver configuration model."""

    name: str = Field(..., description="Driver name")
    driver_class: str = Field(alias="driverClass", description="Driver class path")

    # Configuration sections (allow Any type for flexibility)
    common_config: Optional[Dict[str, Any]] = Field(alias="commonConfig", default_factory=dict)
    producer_config: Optional[Dict[str, Any]] = Field(alias="producerConfig", default_factory=dict)
    consumer_config: Optional[Dict[str, Any]] = Field(alias="consumerConfig", default_factory=dict)
    topic_config: Optional[Dict[str, Any]] = Field(alias="topicConfig", default_factory=dict)

    # Driver-specific settings
    replication_factor: int = Field(alias="replicationFactor", default=1, ge=1)
    reset: bool = Field(default=True, description="Reset topics before test")

    class Config:
        populate_by_name = True

    @validator('common_config', 'producer_config', 'consumer_config', 'topic_config', pre=True)
    def parse_config_string(cls, v):
        """Parse config string into dictionary with proper type conversion."""
        if isinstance(v, str):
            config = {}
            for line in v.strip().split('\n'):
                line = line.strip()
                # Skip empty lines and comments
                if not line or line.startswith('#'):
                    continue

                if '=' not in line:
                    continue

                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()

                # Type conversion
                if value.lower() in ('true', 'false'):
                    # Boolean values
                    config[key] = value.lower() == 'true'
                elif value.isdigit():
                    # Integer values
                    config[key] = int(value)
                elif value.replace('.', '', 1).replace('-', '', 1).isdigit():
                    # Float values (handle negative numbers)
                    try:
                        config[key] = float(value)
                    except ValueError:
                        config[key] = value
                else:
                    # String values (including special ones like 'all', 'latest', etc.)
                    config[key] = value

            print(f"[CONFIG PARSER] Parsed config: {config}", flush=True)
            return config
        return v or {}


class BenchmarkConfig(BaseModel):
    """Main benchmark configuration."""

    # Worker settings
    workers: List[str] = Field(default_factory=list, description="Worker URLs")

    # Logging settings
    log_level: str = Field(default="INFO", description="Logging level")
    log_file: Optional[str] = Field(default=None, description="Log file path")

    # Results settings
    results_dir: str = Field(default="results", description="Results directory")
    results_file_prefix: str = Field(default="benchmark", description="Results file prefix")

    # Monitoring settings
    enable_monitoring: bool = Field(default=True, description="Enable system monitoring")
    monitoring_interval: float = Field(default=1.0, ge=0.1, description="Monitoring interval in seconds")

    # Test settings
    warmup_enabled: bool = Field(default=True, description="Enable warmup phase")
    cleanup_enabled: bool = Field(default=False, description="Enable topic cleanup after test")

    class Config:
        populate_by_name = True


class ConfigLoader:
    """Configuration loader utility."""

    @staticmethod
    def load_workload(file_path: Union[str, Path]) -> WorkloadConfig:
        """Load workload configuration from YAML file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        return WorkloadConfig(**data)

    @staticmethod
    def load_driver(file_path: Union[str, Path]) -> DriverConfig:
        """Load driver configuration from YAML file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        return DriverConfig(**data)

    @staticmethod
    def load_benchmark(file_path: Union[str, Path]) -> BenchmarkConfig:
        """Load benchmark configuration from YAML file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        return BenchmarkConfig(**data)

    @staticmethod
    def save_config(config: BaseModel, file_path: Union[str, Path]) -> None:
        """Save configuration to YAML file."""
        with open(file_path, 'w', encoding='utf-8') as f:
            # Convert back to camelCase for compatibility with Java version
            data = config.dict(by_alias=True, exclude_none=True)
            yaml.dump(data, f, default_flow_style=False, indent=2)


def load_env_config() -> BenchmarkConfig:
    """Load configuration from environment variables."""
    return BenchmarkConfig(
        workers=os.getenv('PY_OMB_WORKERS', '').split(',') if os.getenv('PY_OMB_WORKERS') else [],
        log_level=os.getenv('PY_OMB_LOG_LEVEL', 'INFO'),
        log_file=os.getenv('PY_OMB_LOG_FILE'),
        results_dir=os.getenv('PY_OMB_RESULTS_DIR', 'results'),
        enable_monitoring=os.getenv('PY_OMB_ENABLE_MONITORING', 'true').lower() == 'true',
    )


def merge_configs(base: BenchmarkConfig, override: BenchmarkConfig) -> BenchmarkConfig:
    """Merge two configurations, with override taking precedence."""
    base_dict = base.dict(exclude_none=True)
    override_dict = override.dict(exclude_none=True)
    base_dict.update(override_dict)
    return BenchmarkConfig(**base_dict)