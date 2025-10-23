# Prometheus Monitoring Setup

This directory contains Prometheus and Grafana monitoring configurations for Kafka and Pulsar benchmarks.

## Quick Start

### For Kafka Monitoring

```bash
cd /Users/lbw1125/Desktop/openmessaging-benchmark
docker-compose -f docker-compose-kafka.yml up -d
```

**Access URLs:**
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)
- Kafka JMX Exporter: http://localhost:5556/metrics

### For Pulsar Monitoring

```bash
cd /Users/lbw1125/Desktop/openmessaging-benchmark
docker-compose -f docker-compose-pulsar.yml up -d
```

**Access URLs:**
- Prometheus: http://localhost:9091
- Grafana: http://localhost:3001 (admin/admin)
- Pulsar Metrics: http://localhost:8080/metrics

## Prometheus Queries

### 1. CPU Usage (Process CPU)

**Query:**
```promql
avg(process_cpu_seconds_total) / 1000
```

**For Kafka:**
```promql
avg(process_cpu_seconds_total{job="kafka-jmx"}) / 1000
```

**For Pulsar:**
```promql
avg(process_cpu_seconds_total{job="pulsar"}) / 1000
```

### 2. JVM GC Time

**Query:**
```promql
avg(rate(jvm_gc_collection_seconds_sum[1m]))
```

**For Kafka:**
```promql
avg(rate(jvm_gc_collection_seconds_sum{job="kafka-jmx"}[1m]))
```

**For Pulsar:**
```promql
avg(rate(jvm_gc_collection_seconds_sum{job="pulsar"}[1m]))
```

### 3. JVM Memory Pool Usage

**Query:**
```promql
avg(jvm_memory_pool_bytes_used) / 100000
```

**For Kafka:**
```promql
avg(jvm_memory_pool_bytes_used{job="kafka-jmx"}) / 100000
```

**For Pulsar:**
```promql
avg(jvm_memory_pool_bytes_used{job="pulsar"}) / 100000
```

## Grafana Dashboard Setup

1. Login to Grafana (http://localhost:3000 or http://localhost:3001)
   - Username: `admin`
   - Password: `admin`

2. Add Prometheus data source:
   - Click "Configuration" → "Data Sources"
   - Click "Add data source"
   - Select "Prometheus"
   - For Kafka Grafana: URL = `http://prometheus:9090`
   - For Pulsar Grafana: URL = `http://prometheus:9090`
   - Click "Save & Test"

3. Create Dashboard:
   - Click "+" → "Dashboard" → "Add new panel"
   - Use the queries above
   - Configure visualization as "Time series"

## Available Metrics

### Kafka Metrics (via JMX Exporter)

- `process_cpu_seconds_total` - Process CPU usage
- `jvm_gc_collection_seconds_sum` - GC collection time (counter)
- `jvm_memory_pool_bytes_used` - Memory pool usage by pool name
- `jvm_memory_heap_used` - Heap memory usage
- `kafka_broker_topic_*` - Kafka broker topic metrics
- `kafka_network_request_*` - Kafka network request metrics

### Pulsar Metrics (native Prometheus)

- `process_cpu_seconds_total` - Process CPU usage
- `jvm_gc_collection_seconds_sum` - GC collection time
- `jvm_memory_pool_bytes_used` - Memory pool usage
- `pulsar_*` - Pulsar-specific metrics

## Troubleshooting

### Kafka JMX Exporter not working

Check if JMX is accessible:
```bash
docker exec -it kafka-jmx-exporter curl http://localhost:5556/metrics
```

Check Kafka JMX port:
```bash
docker exec -it kafka nc -zv kafka 9101
```

### Prometheus not scraping

Check Prometheus targets:
- Go to http://localhost:9090/targets (Kafka)
- Go to http://localhost:9091/targets (Pulsar)

All targets should show "UP" status.

### View logs

```bash
# Kafka
docker logs kafka-jmx-exporter
docker logs prometheus-kafka

# Pulsar
docker logs prometheus-pulsar
```

## Stopping Services

```bash
# Stop Kafka monitoring
docker-compose -f docker-compose-kafka.yml down

# Stop Pulsar monitoring
docker-compose -f docker-compose-pulsar.yml down
```

## Data Persistence

Prometheus data is persisted in Docker volumes:
- `prometheus-kafka-data` for Kafka
- `prometheus-pulsar-data` for Pulsar

To remove all data:
```bash
docker volume rm prometheus-kafka-data grafana-kafka-data
docker volume rm prometheus-pulsar-data grafana-pulsar-data
```
