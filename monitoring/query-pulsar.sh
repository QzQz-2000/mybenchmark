#!/bin/bash
# Quick Pulsar metrics query script

PROM_URL="http://localhost:9091"

echo "=== Pulsar Metrics ==="
echo ""

echo "1️⃣  CPU Usage:"
curl -s "${PROM_URL}/api/v1/query" \
  --data-urlencode 'query=avg(process_cpu_seconds_total{job="pulsar"}) / 1000' | \
  python3 -c 'import sys, json; d=json.load(sys.stdin); print(d["data"]["result"][0]["value"][1] if d["data"]["result"] else "No data")'

echo ""
echo "2️⃣  GC Time (rate/1m):"
curl -s "${PROM_URL}/api/v1/query" \
  --data-urlencode 'query=avg(rate(jvm_gc_collection_seconds_sum{job="pulsar"}[1m]))' | \
  python3 -c 'import sys, json; d=json.load(sys.stdin); print(d["data"]["result"][0]["value"][1] if d["data"]["result"] else "No data")'

echo ""
echo "3️⃣  Memory Pool Usage (MB):"
curl -s "${PROM_URL}/api/v1/query" \
  --data-urlencode 'query=avg(jvm_memory_pool_bytes_used{job="pulsar"}) / 1000000' | \
  python3 -c 'import sys, json; d=json.load(sys.stdin); print(d["data"]["result"][0]["value"][1] if d["data"]["result"] else "No data")'

echo ""
