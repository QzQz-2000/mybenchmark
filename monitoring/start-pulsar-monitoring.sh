#!/bin/bash

echo "🚀 Starting Pulsar with Prometheus monitoring..."
echo ""

cd /Users/lbw1125/Desktop/openmessaging-benchmark

# Start services
docker-compose -f docker-compose-pulsar.yml up -d

echo ""
echo "⏳ Waiting for services to start..."
sleep 10

echo ""
echo "✅ Services started!"
echo ""
echo "📊 Access URLs:"
echo "  - Pulsar:             localhost:6650 (binary), localhost:8080 (HTTP)"
echo "  - Pulsar Metrics:     http://localhost:8080/metrics"
echo "  - Prometheus:         http://localhost:9091"
echo "  - Grafana:            http://localhost:3001 (admin/admin)"
echo ""
echo "🔍 Check Prometheus targets:"
echo "  http://localhost:9091/targets"
echo ""
echo "📈 Example Prometheus queries:"
echo "  1. CPU:     avg(process_cpu_seconds_total{job=\"pulsar\"}) / 1000"
echo "  2. GC:      avg(rate(jvm_gc_collection_seconds_sum{job=\"pulsar\"}[1m]))"
echo "  3. Memory:  avg(jvm_memory_pool_bytes_used{job=\"pulsar\"}) / 100000"
echo ""
echo "🛑 To stop: docker-compose -f docker-compose-pulsar.yml down"
