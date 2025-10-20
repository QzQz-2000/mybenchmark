# Distributed Deployment Guide

This guide explains how to run the OpenMessaging Benchmark framework in distributed mode across multiple machines.

## Architecture Overview

The distributed architecture consists of:

1. **Controller Node**: The machine where you run the benchmark command. It coordinates all workers.
2. **Worker Nodes**: Multiple machines that run the actual producers and consumers.

```
┌─────────────┐
│ Controller  │
│   Node      │
└──────┬──────┘
       │
       ├──────────┬──────────┬──────────┐
       │          │          │          │
   ┌───▼───┐  ┌──▼────┐  ┌──▼────┐  ┌──▼────┐
   │Worker1│  │Worker2│  │Worker3│  │Worker4│
   │ :8080 │  │ :8080 │  │ :8080 │  │ :8080 │
   └───────┘  └───────┘  └───────┘  └───────┘
```

## Prerequisites

### On All Nodes (Controller + Workers)

1. **Python 3.7+**
2. **Install dependencies**:
   ```bash
   cd /path/to/openmessaging-benchmark
   pip install -e .
   ```

3. **Network connectivity**: All worker nodes must be accessible from the controller node via HTTP.

### Kafka/Pulsar Cluster

Ensure you have a Kafka or Pulsar cluster running that is accessible from all worker nodes.

## Step-by-Step Setup

### Step 1: Start Worker Nodes

On each worker machine, start a worker process:

```bash
# Worker 1 (on machine 1)
cd /path/to/openmessaging-benchmark
./start_worker.sh 8080

# Worker 2 (on machine 2)
cd /path/to/openmessaging-benchmark
./start_worker.sh 8080

# Worker 3 (on machine 3)
cd /path/to/openmessaging-benchmark
./start_worker.sh 8080
```

**Alternative - Python command**:
```bash
python -m benchmark.worker.benchmark_worker --port 8080 --stats-port 8081
```

**Alternative - Multiple workers on same machine** (for testing):
```bash
# Terminal 1
./start_worker.sh 8080

# Terminal 2
./start_worker.sh 8081

# Terminal 3
./start_worker.sh 8082
```

You should see:
```
2025-10-18 10:00:00 - benchmark.worker.benchmark_worker - INFO - Starting benchmark worker on port 8080
 * Serving Flask app 'benchmark_worker'
 * Running on http://0.0.0.0:8080
```

### Step 2: Configure Workers List

Create a workers configuration file (or use `examples/distributed-workers.yaml`):

```yaml
# distributed-workers.yaml
workers:
  - http://worker1.example.com:8080
  - http://worker2.example.com:8080
  - http://worker3.example.com:8080
```

**For local testing** (multiple workers on localhost):
```yaml
workers:
  - http://localhost:8080
  - http://localhost:8081
  - http://localhost:8082
```

### Step 3: Prepare Driver Configuration

Ensure your driver configuration (e.g., `kafka-driver.yaml`) points to the correct Kafka/Pulsar cluster:

```yaml
# examples/kafka-driver.yaml
name: Kafka
driverClass: benchmark.driver_kafka.kafka_benchmark_driver.KafkaBenchmarkDriver

replicationFactor: 3

commonConfig:
  bootstrap.servers: kafka-broker1:9092,kafka-broker2:9092,kafka-broker3:9092

producerConfig:
  acks: all
  linger.ms: 1

consumerConfig:
  auto.offset.reset: earliest
  enable.auto.commit: false
```

### Step 4: Run Distributed Benchmark

From the controller node:

```bash
python -m benchmark.benchmark \
  -d examples/kafka-driver.yaml \
  -wf distributed-workers.yaml \
  examples/distributed-workload.yaml
```

**Or with command-line worker specification**:
```bash
python -m benchmark.benchmark \
  -d examples/kafka-driver.yaml \
  -w http://worker1:8080 http://worker2:8080 http://worker3:8080 \
  examples/distributed-workload.yaml
```

### Step 5: Monitor Progress

You'll see logs on the controller node:
```
2025-10-18 10:00:00 - benchmark.benchmark - INFO - Starting benchmark with config: {...}
2025-10-18 10:00:01 - benchmark.worker.distributed_workers_ensemble - INFO - Ensemble of distributed workers (3 workers)
2025-10-18 10:00:02 - benchmark.workload_generator - INFO - Creating 10 topics...
2025-10-18 10:00:05 - benchmark.workload_generator - INFO - Creating producers distributed across 3 workers...
2025-10-18 10:00:10 - benchmark.workload_generator - INFO - Creating consumers distributed across 3 workers...
2025-10-18 10:00:15 - benchmark.workload_generator - INFO - Starting load test...
```

Each worker node will also show activity:
```
2025-10-18 10:00:05 - benchmark.worker.local_worker - INFO - Creating 3 producers...
2025-10-18 10:00:10 - benchmark.worker.local_worker - INFO - Creating 3 consumers...
2025-10-18 10:00:15 - benchmark.worker.local_worker - INFO - Starting 3 Producer Agent processes
```

## How Workload is Distributed

The framework automatically distributes the workload across workers using **round-robin partitioning**:

### Example: 10 Topics, 3 Workers

- **Worker 1**: Topics 0, 3, 6, 9
- **Worker 2**: Topics 1, 4, 7
- **Worker 3**: Topics 2, 5, 8

Each worker creates:
- Producers for its assigned topics
- Consumers for its assigned subscriptions

### Statistics Aggregation

The controller node automatically aggregates statistics from all workers:
- Message counts (sent/received)
- Throughput (messages/sec, bytes/sec)
- Latency histograms (p50, p95, p99, etc.)

## Configuration Tips

### Scaling Guidelines

| Workers | Topics | Producers/Topic | Total Throughput | Use Case |
|---------|--------|-----------------|------------------|----------|
| 1       | 10     | 1               | ~10K msg/s       | Testing  |
| 3       | 30     | 2               | ~100K msg/s      | Medium   |
| 5       | 50     | 5               | ~500K msg/s      | Large    |
| 10      | 100    | 10              | ~1M msg/s        | Extreme  |

### Network Requirements

- **Bandwidth**: Each worker may generate/consume significant data (e.g., 100 MB/s at high throughput)
- **Latency**: Low latency between workers and Kafka/Pulsar cluster is crucial
- **Ports**: Default worker port is 8080 (configurable)

### Resource Requirements per Worker

- **CPU**: 4-8 cores recommended
- **Memory**: 4-8 GB depending on workload
- **Network**: 1 Gbps+ recommended for high throughput tests

## Troubleshooting

### Worker Connection Errors

**Problem**: `ConnectionError: Unable to connect to worker`

**Solution**:
1. Verify worker is running: `curl http://worker1:8080/counters-stats`
2. Check firewall rules allow traffic on port 8080
3. Ensure worker address in `workers.yaml` is correct

### Worker Timeout

**Problem**: `Timeout waiting for worker response`

**Solution**:
1. Increase timeout in `HttpWorkerClient` (default: 30s)
2. Check worker logs for errors
3. Verify Kafka/Pulsar cluster is accessible from worker

### Uneven Load Distribution

**Problem**: Some workers have much higher load than others

**Solution**:
1. Ensure topic count is divisible by worker count (or higher)
2. Increase `partitionsPerTopic` for better distribution
3. Check worker machine resources (CPU, memory, network)

### Statistics Mismatch

**Problem**: Messages sent != messages received

**Solution**:
1. Check for errors in worker logs
2. Ensure sufficient test duration for all messages to be consumed
3. Verify consumer configuration (e.g., `auto.offset.reset`)

## Advanced Configuration

### Custom Worker Ports

```bash
# Worker 1 on custom port
./start_worker.sh 9090

# In workers.yaml
workers:
  - http://worker1:9090
```

### Extra Consumer Workers

For backlog scenarios, allocate extra workers for consumers:

```bash
python -m benchmark.benchmark \
  -d examples/kafka-driver.yaml \
  -wf distributed-workers.yaml \
  -x \
  examples/backlog-workload.yaml
```

The `-x` flag allocates additional workers for consumers when backlog builds up.

### SSL/TLS Configuration

If workers are exposed over the internet, consider using:
1. **Reverse proxy** (nginx, HAProxy) with SSL termination
2. **VPN** for secure communication between nodes
3. **Firewall rules** to restrict access to worker ports

## Example: 3-Node Distributed Test

### Setup

**Machine 1** (192.168.1.10):
```bash
./start_worker.sh 8080
```

**Machine 2** (192.168.1.11):
```bash
./start_worker.sh 8080
```

**Machine 3** (192.168.1.12):
```bash
./start_worker.sh 8080
```

**Controller** (192.168.1.100):
```bash
# Create workers.yaml
cat > workers.yaml <<EOF
workers:
  - http://192.168.1.10:8080
  - http://192.168.1.11:8080
  - http://192.168.1.12:8080
EOF

# Run benchmark
python -m benchmark.benchmark \
  -d examples/kafka-driver.yaml \
  -wf workers.yaml \
  examples/distributed-workload.yaml
```

### Expected Results

With 10 topics, 1 producer per topic, 1000 msg/s per producer:

- **Total throughput**: 10,000 msg/s
- **Per worker**: ~3,333 msg/s (topics distributed evenly)
- **Test duration**: 5 minutes
- **Total messages**: ~3,000,000

Results will be saved to a JSON file like:
```
distributed-test-workload-Kafka-2025-10-18-10-30-45.json
```

## Monitoring

### Health Check

Check worker status:
```bash
curl http://worker1:8080/counters-stats
```

Expected response:
```json
{
  "messagesSent": 100000,
  "messagesReceived": 100000,
  "messageSendErrors": 0
}
```

### Real-time Monitoring

Watch worker logs for issues:
```bash
# On worker node
tail -f /var/log/benchmark-worker.log
```

## Next Steps

- Review [README.md](README.md) for workload configuration options
- See [examples/](examples/) for more workload examples
- Check [benchmark_results/](benchmark_results/) for result analysis

## Support

For issues or questions:
1. Check worker logs for error messages
2. Verify network connectivity between all nodes
3. Ensure Kafka/Pulsar cluster is healthy
4. Review this documentation for common issues
