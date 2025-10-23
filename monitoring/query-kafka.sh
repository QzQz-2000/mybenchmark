#!/bin/bash
# Quick Kafka metrics query script

PROM_URL="http://localhost:9090"

echo "=== Kafka Metrics ==="
echo ""

echo "1️⃣  CPU Usage:"
curl -s "${PROM_URL}/api/v1/query" \
  --data-urlencode 'query=avg(process_cpu_seconds_total{job="kafka-jmx"}) / 1000' | \
  python3 -c 'import sys, json; d=json.load(sys.stdin); print(d["data"]["result"][0]["value"][1] if d["data"]["result"] else "No data")'

echo ""
echo "2️⃣  GC Time (rate/1m):"
curl -s "${PROM_URL}/api/v1/query" \
  --data-urlencode 'query=avg(rate(jvm_gc_collection_seconds_sum{job="kafka-jmx"}[1m]))' | \
  python3 -c 'import sys, json; d=json.load(sys.stdin); print(d["data"]["result"][0]["value"][1] if d["data"]["result"] else "No data")'

echo ""
echo "3️⃣  Memory Pool Usage (MB):"
curl -s "${PROM_URL}/api/v1/query" \
  --data-urlencode 'query=avg(jvm_memory_pool_bytes_used{job="kafka-jmx"}) / 1000000' | \
  python3 -c 'import sys, json; d=json.load(sys.stdin); print(d["data"]["result"][0]["value"][1] if d["data"]["result"] else "No data")'

echo ""
