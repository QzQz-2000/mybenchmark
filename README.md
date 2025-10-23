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

python -m benchmark -d examples/kafka-driver-max-throughput.yaml workloads/slow-consumer-high-parallelism.yaml

docker compose -f docker-compose-kafka.yml up -d
docker compose -f docker-compose-pulsar.yml up -d



python -m benchmark.benchmark \
    -d examples/kafka-driver.yaml \
    workloads/test-max-throughput.yaml


============================================================================================================================================
测试结果汇总 - 生产速率扩展性测试
============================================================================================================================================
目标速率         LatencyAvg(ms)     Latency50(ms)      Latency75(ms)      Latency95(ms)      Latency99(ms)      ProdRate     ConsRate     ProdThr(MB/s)   ConsThr(MB/s)  
--------------------------------------------------------------------------------------------------------------------------------------------
1000msg/s    80.27±0.00        70.00±0.00        101.00±0.00       174.00±0.00       274.00±0.00       978.8±0.0   959.2±0.0   0.96±0.00      0.94±0.00      
2000msg/s    74.18±0.00        75.00±0.00        101.00±0.00       142.00±0.00       211.00±0.00       1985.2±0.0  1994.9±0.0  1.94±0.00      1.95±0.00      
============================================================================================================================================

============================================================================================================================================
测试结果汇总 - 生产速率扩展性测试
============================================================================================================================================
目标速率         LatencyAvg(ms)     Latency50(ms)      Latency75(ms)      Latency95(ms)      Latency99(ms)      ProdRate     ConsRate     ProdThr(MB/s)   ConsThr(MB/s)  
--------------------------------------------------------------------------------------------------------------------------------------------
1000msg/s    1550.11±0.00      1691.00±0.00      1936.00±0.00      2509.00±0.00      2659.00±0.00      958.7±0.0   982.3±0.0   0.94±0.00      0.96±0.00      
2000msg/s    833.01±0.00       800.00±0.00       1158.00±0.00      1562.00±0.00      1830.00±0.00      1968.6±0.0  1927.3±0.0  1.92±0.00      1.88±0.00      
4000msg/s    1128.50±0.00      1056.00±0.00      1657.00±0.00      2203.00±0.00      2401.00±0.00      3927.4±0.0  3912.0±0.0  3.84±0.00      3.82±0.00      
============================================================================================================================================

============================================================================================================================================
测试结果汇总 - 生产速率扩展性测试
============================================================================================================================================
目标速率         LatencyAvg(ms)     Latency50(ms)      Latency75(ms)      Latency95(ms)      Latency99(ms)      ProdRate     ConsRate     ProdThr(MB/s)   ConsThr(MB/s)  
--------------------------------------------------------------------------------------------------------------------------------------------
1000msg/s    1163.70±0.00      1149.00±0.00      1483.00±0.00      1860.00±0.00      2360.00±0.00      970.3±0.0   977.4±0.0   0.95±0.00      0.95±0.00      
2000msg/s    635.70±0.00       662.00±0.00       878.00±0.00       1100.00±0.00      1318.00±0.00      1972.5±0.0  1970.2±0.0  1.93±0.00      1.92±0.00      
4000msg/s    756.16±0.00       674.00±0.00       960.00±0.00       1614.00±0.00      1774.00±0.00      3915.2±0.0  3919.4±0.0  3.82±0.00      3.83±0.00      
============================================================================================================================================