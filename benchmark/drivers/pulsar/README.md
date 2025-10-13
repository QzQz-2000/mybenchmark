# Pulsar Benchmark Driver

Python implementation of the Pulsar driver for OpenMessaging Benchmark.

## Prerequisites

1. **Install Apache Pulsar Python client:**
   ```bash
   pip install pulsar-client
   ```

2. **Start Pulsar standalone (for testing):**
   ```bash
   # Download Pulsar
   wget https://archive.apache.org/dist/pulsar/pulsar-3.0.0/apache-pulsar-3.0.0-bin.tar.gz
   tar xvfz apache-pulsar-3.0.0-bin.tar.gz
   cd apache-pulsar-3.0.0

   # Start Pulsar standalone
   bin/pulsar standalone
   ```

## Configuration

See `examples/pulsar-driver.yaml` for configuration options.

Key configuration parameters:

### Client Configuration
- **client.serviceUrl**: Pulsar service URL (default: `pulsar://localhost:6650`)
- **client.httpUrl**: Pulsar admin HTTP URL (default: `http://localhost:8080`)
- **client.namespacePrefix**: Base namespace path (default: `benchmark/ns`)
- **client.useFixedNamespace**: Enable idempotent test mode (default: `false`)
  - `true`: Use fixed namespace, topics deleted before/after each test (idempotent, recommended)
  - `false`: Use random namespace suffix, topics accumulate (legacy behavior)
- **client.topicType**: Topic type (default: `persistent`)

### Producer Configuration
- **producer.batchingEnabled**: Enable producer batching (default: `true`)
- **producer.batchingMaxPublishDelayMs**: Max batching delay in ms (default: `1`)
- **producer.blockIfQueueFull**: Block if producer queue is full (default: `true`)
- **producer.pendingQueueSize**: Max pending messages (default: `1000`)

### Consumer Configuration
- **consumer.subscriptionType**: Subscription type (options: `Exclusive`, `Shared`, `Failover`, `Key_Shared`)
- **consumer.receiverQueueSize**: Consumer receive queue size (default: `1000`)

## Usage

### Test Driver (without full benchmark)

```bash
python test_pulsar_driver.py
```

### Run Full Benchmark

```bash
python benchmark.py \
    --driver examples/pulsar-driver.yaml \
    --workload workloads/1-topic-1-partition-1kb.yaml
```

## Architecture

### Files

- `pulsar_benchmark_driver.py`: Main driver implementation
- `pulsar_benchmark_producer.py`: Producer wrapper
- `pulsar_benchmark_consumer.py`: Consumer wrapper

### Key Features

1. **Auto-create Topics**: Topics are automatically created on first use
2. **Batching Support**: Producer batching for better throughput
3. **Multiple Subscription Types**: Supports Exclusive, Shared, Failover, and Key_Shared
4. **TLS Support**: Supports TLS/SSL connections
5. **Authentication**: Supports Pulsar authentication plugins

## Troubleshooting

### Common Issues

1. **"pulsar-client not installed"**
   ```bash
   pip install pulsar-client
   ```

2. **"Connection refused"**
   - Check if Pulsar is running: `curl http://localhost:8080/admin/v2/clusters`
   - Verify serviceUrl in config matches your Pulsar setup

3. **"Namespace not found"**
   - The driver will auto-create namespaces
   - For production, pre-create namespace: `bin/pulsar-admin namespaces create benchmark/ns`

## Comparison with Java Driver

The Python driver aims to provide feature parity with the Java driver:

| Feature | Java Driver | Python Driver | Notes |
|---------|-------------|---------------|-------|
| Basic produce/consume | ✓ | ✓ | |
| Batching | ✓ | ✓ | |
| Partitioned topics | ✓ | ⚠️ | Auto-creation limited |
| TLS/SSL | ✓ | ✓ | |
| Authentication | ✓ | ✓ | Token auth supported |
| Admin operations | ✓ | ⚠️ | Limited (requires HTTP API) |
| Message listener | ✓ | ✓ | |

## Performance Notes

- The Python Pulsar client is a wrapper around the C++ client
- Performance should be similar to Java for high throughput scenarios
- For best performance:
  - Enable batching (`batchingEnabled: true`)
  - Tune `batchingMaxPublishDelayMs` based on your latency requirements
  - Adjust `receiverQueueSize` for consumers

## References

- [Apache Pulsar Python Client](https://pulsar.apache.org/docs/client-libraries-python/)
- [Pulsar Documentation](https://pulsar.apache.org/docs/)
- [OpenMessaging Benchmark](https://github.com/openmessaging/openmessaging-benchmark)
