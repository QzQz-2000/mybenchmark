# Python OpenMessaging Benchmark Usage Guide

This guide covers how to use the Python OpenMessaging Benchmark framework to test Kafka and other messaging systems.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Running Benchmarks](#running-benchmarks)
5. [Understanding Results](#understanding-results)
6. [Advanced Usage](#advanced-usage)
7. [Troubleshooting](#troubleshooting)

## Quick Start

### 1. Start Kafka (using Docker)

```bash
# Start Kafka cluster
docker-compose up -d kafka zookeeper

# Wait for Kafka to be ready
docker-compose logs kafka | grep "started (kafka.server.KafkaServer)"
```

### 2. Start Workers

```bash
# Terminal 1: Start first worker with kafka-python
python scripts/start_worker.py kafka \
  --worker-id worker-1 \
  --driver-config configs/kafka-throughput.yaml \
  --client-type kafka-python \
  --port 8080

# Terminal 2: Start second worker with confluent-kafka
python scripts/start_worker.py kafka \
  --worker-id worker-2 \
  --driver-config configs/kafka-throughput.yaml \
  --client-type confluent-kafka \
  --port 8081
```

### 3. Run Benchmark

```bash
python scripts/run_benchmark.py run \
  --workload workloads/1-topic-1-partition-1kb.yaml \
  --driver configs/kafka-throughput.yaml \
  --workers http://localhost:8080 http://localhost:8081 \
  --client-type kafka-python
```

## Installation

### Prerequisites

- Python 3.9+
- Kafka cluster (local or remote)
- Docker (optional, for local Kafka)

### Install Dependencies

```bash
# Clone the repository
git clone <repository-url>
cd py-openmessaging-benchmark

# Install Python dependencies
pip install -r requirements.txt

# For development
pip install -r requirements-dev.txt

# Install in development mode
pip install -e .
```

### Install Kafka Python Clients

```bash
# Install all supported clients
pip install kafka-python confluent-kafka aiokafka

# Or install individually
pip install kafka-python      # Apache Kafka Python client
pip install confluent-kafka   # Confluent's Python client
pip install aiokafka          # Async Kafka client
```

### Check Client Availability

```bash
python scripts/run_benchmark.py check-clients
```

## Configuration

### Driver Configuration

Driver configurations define how to connect to and configure the messaging system.

**Example: `configs/kafka-throughput.yaml`**

```yaml
name: Kafka
driverClass: benchmark.drivers.kafka.KafkaDriver

# Kafka-specific settings
replicationFactor: 3

# Topic configuration
topicConfig: |
  min.insync.replicas=2
  cleanup.policy=delete

# Common settings for all clients
commonConfig: |
  bootstrap.servers=localhost:9092
  default.api.timeout.ms=120000

# Producer-specific settings
producerConfig: |
  acks=all
  linger.ms=1
  batch.size=1048576
  compression.type=gzip

# Consumer-specific settings
consumerConfig: |
  auto.offset.reset=earliest
  enable.auto.commit=false
  max.partition.fetch.bytes=10485760
```

### Workload Configuration

Workload configurations define the test scenario.

**Example: `workloads/1-topic-1-partition-1kb.yaml`**

```yaml
name: 1 topic / 1 partition / 1Kb

# Topic setup
topics: 1
partitionsPerTopic: 1
keyDistributor: "NO_KEY"  # NO_KEY, RANDOM, ROUND_ROBIN, ZIP_LATENT

# Message configuration
messageSize: 1024
payloadFile: "payload/payload-1Kb.data"  # Optional custom payload

# Producer configuration
producersPerTopic: 1
producerRate: 50000  # messages per second

# Consumer configuration
subscriptionsPerTopic: 1
consumerPerSubscription: 1
consumerBacklogSizeGB: 0

# Test duration
testDurationMinutes: 15
warmupDurationMinutes: 1
```

### Key Distributor Options

- `NO_KEY`: No message keys
- `RANDOM`: Random keys
- `ROUND_ROBIN`: Keys distributed round-robin
- `ZIP_LATENT`: Zipfian distribution (simulates real-world key patterns)

## Running Benchmarks

### Basic Benchmark

```bash
python scripts/run_benchmark.py run \
  --workload workloads/1-topic-1-partition-1kb.yaml \
  --driver configs/kafka-throughput.yaml \
  --workers http://localhost:8080 \
  --output-dir results
```

### Multi-Client Comparison

```bash
# Start workers with different clients
python scripts/start_worker.py kafka --worker-id w1 --client-type kafka-python --port 8080 &
python scripts/start_worker.py kafka --worker-id w2 --client-type confluent-kafka --port 8081 &
python scripts/start_worker.py kafka --worker-id w3 --client-type aiokafka --port 8082 &

# Run comparison test
python scripts/run_benchmark.py run \
  --workload workloads/python-client-comparison.yaml \
  --driver configs/kafka-throughput.yaml \
  --workers http://localhost:8080 http://localhost:8081 http://localhost:8082
```

### Latency Testing

```bash
python scripts/run_benchmark.py run \
  --workload workloads/latency-test-100b.yaml \
  --driver configs/kafka-latency.yaml \
  --workers http://localhost:8080 \
  --client-type confluent-kafka
```

### High Throughput Testing

```bash
python scripts/run_benchmark.py run \
  --workload workloads/high-throughput-test.yaml \
  --driver configs/kafka-throughput.yaml \
  --workers http://localhost:8080 http://localhost:8081 http://localhost:8082 \
  --enable-monitoring
```

## Understanding Results

### Result Files

Each benchmark run generates several files:

- `benchmark_<test_id>.json`: Raw results in JSON format
- `benchmark_<test_id>.csv`: Summary results in CSV format
- `system_metrics.png`: System resource usage graphs (if monitoring enabled)

### Key Metrics

**Throughput Metrics:**
- Messages per second (msg/s)
- Bandwidth in MB/s
- Total messages processed

**Latency Metrics:**
- Average latency
- P50, P95, P99, P99.9 percentiles
- Min/max latency

**System Metrics:**
- CPU usage
- Memory usage
- Network I/O
- Disk I/O

### Sample Output

```
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃                                              Benchmark Results Summary                                               ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Metric                      ┃ Value                                                                                  ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ Test Name                   │ 1 topic / 1 partition / 1Kb                                                           │
│ Duration                    │ 900.2s                                                                                 │
│ Producers                   │ 1                                                                                      │
│ Consumers                   │ 1                                                                                      │
│ Messages Sent               │ 45,000,000                                                                             │
│ Producer Throughput         │ 50,000 msg/s                                                                          │
│ Producer Bandwidth          │ 48.83 MB/s                                                                            │
│ Messages Consumed           │ 44,999,950                                                                             │
│ Consumer Throughput         │ 49,999 msg/s                                                                          │
│ Consumer Bandwidth          │ 48.83 MB/s                                                                            │
│ Avg Latency                 │ 2.35ms                                                                                 │
│ P95 Latency                 │ 8.92ms                                                                                 │
│ P99 Latency                 │ 15.67ms                                                                                │
│ Avg CPU                     │ 45.2%                                                                                  │
│ Max CPU                     │ 78.9%                                                                                  │
│ Avg Memory                  │ 62.1%                                                                                  │
└─────────────────────────────┴────────────────────────────────────────────────────────────────────────────────────┘
```

## Advanced Usage

### Custom Driver Configuration

Create custom driver configurations for specific scenarios:

**Low Latency Configuration:**

```yaml
name: Kafka Low Latency
driverClass: benchmark.drivers.kafka.KafkaDriver

replicationFactor: 3

producerConfig: |
  acks=all
  linger.ms=0
  batch.size=1
  compression.type=none

consumerConfig: |
  fetch.min.bytes=1
  fetch.max.wait.ms=0
```

**High Throughput Configuration:**

```yaml
name: Kafka High Throughput
driverClass: benchmark.drivers.kafka.KafkaDriver

replicationFactor: 3

producerConfig: |
  acks=1
  linger.ms=100
  batch.size=1048576
  compression.type=lz4
  buffer.memory=67108864

consumerConfig: |
  fetch.min.bytes=1048576
  fetch.max.wait.ms=500
  max.partition.fetch.bytes=10485760
```

### Environment Variables

Configure the benchmark using environment variables:

```bash
export PY_OMB_WORKERS="http://localhost:8080,http://localhost:8081"
export PY_OMB_LOG_LEVEL="DEBUG"
export PY_OMB_RESULTS_DIR="/path/to/results"
export PY_OMB_ENABLE_MONITORING="true"
```

### Payload Files

Use custom message payloads:

```bash
# Create custom payload
echo "Custom message payload data" > payload/custom.data

# Reference in workload config
payloadFile: "payload/custom.data"
```

### Worker Health Monitoring

Check worker health:

```bash
# Check all workers
python scripts/start_worker.py health-check \
  --worker-urls http://localhost:8080 http://localhost:8081

# Get detailed stats from a worker
python scripts/start_worker.py stats --worker-url http://localhost:8080
```

### Running with Docker

```bash
# Build the image
docker build -t py-omb .

# Start complete environment
docker-compose up

# Run benchmark against containerized workers
python scripts/run_benchmark.py run \
  --workload workloads/1-topic-1-partition-1kb.yaml \
  --driver configs/kafka-throughput.yaml \
  --workers http://localhost:8080 http://localhost:8081 http://localhost:8082
```

## Troubleshooting

### Common Issues

**Worker Connection Failures:**

```bash
# Check worker health
curl http://localhost:8080/health

# Check worker logs
docker-compose logs worker-1
```

**Kafka Connection Issues:**

```bash
# Test Kafka connectivity
python -c "
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
print('Kafka connection successful')
"
```

**Missing Dependencies:**

```bash
# Check client availability
python scripts/run_benchmark.py check-clients

# Install missing clients
pip install kafka-python confluent-kafka aiokafka
```

### Performance Tuning

**For Low Latency:**
- Use `batch.size=1` and `linger.ms=0`
- Disable compression
- Use `acks=1` for faster acknowledgment
- Minimize network hops

**For High Throughput:**
- Increase `batch.size` (16KB-1MB)
- Increase `linger.ms` (10-100ms)
- Enable compression (`gzip`, `lz4`, `zstd`)
- Use multiple partitions
- Tune `buffer.memory` for producers

**For Reliability:**
- Use `acks=all`
- Enable `enable.idempotence=true`
- Set appropriate `retries`
- Use `min.insync.replicas >= 2`

### Debugging

Enable debug logging:

```bash
python scripts/run_benchmark.py run \
  --log-level DEBUG \
  --log-file debug.log \
  ...
```

Check worker logs:

```bash
# Worker logs show detailed execution
tail -f worker.log
```

Validate configurations:

```bash
python scripts/run_benchmark.py validate \
  --workload workloads/1-topic-1-partition-1kb.yaml \
  --driver configs/kafka-throughput.yaml
```

## Best Practices

1. **Warm-up Phase**: Always use warm-up to stabilize performance
2. **Multiple Runs**: Run tests multiple times and average results
3. **System Monitoring**: Enable monitoring for system resource tracking
4. **Realistic Scenarios**: Use workloads that match your production patterns
5. **Client Comparison**: Test multiple Python clients to find the best fit
6. **Resource Limits**: Monitor system resources to avoid bottlenecks
7. **Network Considerations**: Account for network latency in distributed setups

## Support

- Check the [GitHub Issues](https://github.com/openmessaging/py-openmessaging-benchmark/issues) for known problems
- Review the [API Documentation](API.md) for advanced usage
- See [Examples](examples/) directory for more scenarios