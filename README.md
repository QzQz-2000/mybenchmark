# OpenMessaging Benchmark - Python Version

Python implementation of the OpenMessaging benchmark framework with Kafka driver support.

## Installation

```bash
cd /Users/lbw1125/Desktop/openmessaging-benchmark
pip install -e .
```

## Quick Start

### 1. Start Kafka

Make sure you have Kafka running locally on `localhost:9092`:

```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka
bin/kafka-server-start.sh config/server.properties
```

### 2. Run Benchmark

```bash
# Run with local worker (no distributed setup needed)
cd /Users/lbw1125/Desktop/openmessaging-benchmark
python -m benchmark.benchmark \
  -d examples/kafka-driver.yaml \
  examples/simple-workload.yaml
```

Or with explicit workers file:

```bash
python -m benchmark.benchmark \
  -d examples/kafka-driver.yaml \
  -wf examples/workers.yaml \
  examples/simple-workload.yaml
```

## Configuration Files

### Driver Configuration (`kafka-driver.yaml`)

Defines the messaging system driver and its settings:
- `driverClass`: Python class path to the driver
- `replicationFactor`: Kafka replication factor
- `commonConfig`: Common Kafka client settings
- `producerConfig`: Producer-specific settings
- `consumerConfig`: Consumer-specific settings

### Workload Configuration (`simple-workload.yaml`)

Defines the benchmark workload:
- `topics`: Number of topics
- `partitionsPerTopic`: Partitions per topic
- `messageSize`: Message size in bytes
- `producerRate`: Messages per second (0 = max rate)
- `testDurationMinutes`: Test duration
- etc.

### Workers Configuration (`workers.yaml`)

For distributed benchmarks, list worker URLs. Leave empty for local mode.

## Architecture

```
benchmark/
├── benchmark.py           # Main entry point
├── workload_generator.py  # Workload execution engine
├── driver/                # Driver API interfaces
├── driver_kafka/          # Kafka driver implementation
├── worker/                # Worker framework (local & distributed)
├── utils/                 # Utilities (rate limiter, timers, etc.)
└── tool/                  # Workload generation tools
```

## Requirements

- Python 3.7+
- Kafka 2.0+ (kafka-python library)
- PyYAML
- hdrhistogram

See `requirements.txt` for full list.

## License

Apache License 2.0


docker run -it \
  -p 6650:6650 \
  -p 8080:8080 \
  --name pulsar-standalone \
  apachepulsar/pulsar:3.2.0 \
  bin/pulsar standalone


python -m benchmark.benchmark \
      -d examples/pulsar-driver.yaml \
      workloads/1-topic-1-partition-1kb.yaml


python -m benchmark.benchmark \
      -d examples/kafka-driver.yaml \
      workloads/1-topic-1-partition-1kb.yaml