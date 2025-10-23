#!/bin/bash

echo "🚀 Starting Kafka with Prometheus monitoring..."
echo ""

cd /Users/lbw1125/Desktop/openmessaging-benchmark

# Start services
docker-compose -f docker-compose-kafka.yml up -d

echo ""
echo "⏳ Waiting for services to start..."
sleep 10

echo ""
echo "✅ Services started!"
echo ""
echo "📊 Access URLs:"
echo "  - Kafka:              localhost:9092"
echo "  - JMX Exporter:       http://localhost:5556/metrics"
echo "  - Prometheus:         http://localhost:9090"
echo "  - Grafana:            http://localhost:3000 (admin/admin)"
echo ""
echo "🔍 Check Prometheus targets:"
echo "  http://localhost:9090/targets"
echo ""
echo "📈 Example Prometheus queries:"
echo "  1. CPU:     avg(process_cpu_seconds_total{job=\"kafka-jmx\"}) / 1000"
echo "  2. GC:      avg(rate(jvm_gc_collection_seconds_sum{job=\"kafka-jmx\"}[1m]))"
echo "  3. Memory:  avg(jvm_memory_pool_bytes_used{job=\"kafka-jmx\"}) / 100000"
echo ""
echo "🛑 To stop: docker-compose -f docker-compose-kafka.yml down"
