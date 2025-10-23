#!/bin/bash

# Prometheus Query Examples using curl
# 使用 curl 查询 Prometheus 的示例脚本

# 颜色定义
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}==============================================================${NC}"
echo -e "${BLUE}Prometheus Query Examples (curl)${NC}"
echo -e "${BLUE}==============================================================${NC}"
echo ""

# 检测使用 Kafka 还是 Pulsar
if [ "$1" == "kafka" ]; then
    PROM_URL="http://localhost:9090"
    JOB="kafka-jmx"
    echo -e "${GREEN}Target: Kafka Prometheus (port 9090)${NC}"
elif [ "$1" == "pulsar" ]; then
    PROM_URL="http://localhost:9091"
    JOB="pulsar"
    echo -e "${GREEN}Target: Pulsar Prometheus (port 9091)${NC}"
else
    echo -e "${YELLOW}Usage: $0 [kafka|pulsar]${NC}"
    echo ""
    echo "Examples:"
    echo "  $0 kafka   # Query Kafka metrics"
    echo "  $0 pulsar  # Query Pulsar metrics"
    exit 1
fi

echo ""
echo -e "${BLUE}--------------------------------------------------------------${NC}"
echo -e "${BLUE}1. CPU Usage (Process CPU)${NC}"
echo -e "${BLUE}--------------------------------------------------------------${NC}"
echo -e "${YELLOW}Query:${NC} avg(process_cpu_seconds_total{job=\"$JOB\"}) / 1000"
echo ""

curl -s "${PROM_URL}/api/v1/query" \
  --data-urlencode "query=avg(process_cpu_seconds_total{job=\"$JOB\"}) / 1000" | \
  python3 -m json.tool

echo ""
echo ""
echo -e "${BLUE}--------------------------------------------------------------${NC}"
echo -e "${BLUE}2. JVM GC Collection Time (rate over 1 minute)${NC}"
echo -e "${BLUE}--------------------------------------------------------------${NC}"
echo -e "${YELLOW}Query:${NC} avg(rate(jvm_gc_collection_seconds_sum{job=\"$JOB\"}[1m]))"
echo ""

curl -s "${PROM_URL}/api/v1/query" \
  --data-urlencode "query=avg(rate(jvm_gc_collection_seconds_sum{job=\"$JOB\"}[1m]))" | \
  python3 -m json.tool

echo ""
echo ""
echo -e "${BLUE}--------------------------------------------------------------${NC}"
echo -e "${BLUE}3. JVM Memory Pool Usage (in MB)${NC}"
echo -e "${BLUE}--------------------------------------------------------------${NC}"
echo -e "${YELLOW}Query:${NC} avg(jvm_memory_pool_bytes_used{job=\"$JOB\"}) / 100000"
echo ""

curl -s "${PROM_URL}/api/v1/query" \
  --data-urlencode "query=avg(jvm_memory_pool_bytes_used{job=\"$JOB\"}) / 100000" | \
  python3 -m json.tool

echo ""
echo ""
echo -e "${BLUE}--------------------------------------------------------------${NC}"
echo -e "${BLUE}4. All Available Metrics (first 10)${NC}"
echo -e "${BLUE}--------------------------------------------------------------${NC}"
echo ""

curl -s "${PROM_URL}/api/v1/label/__name__/values" | \
  python3 -c "import sys, json; data=json.load(sys.stdin); print('\n'.join(data['data'][:10]))"

echo ""
echo ""
echo -e "${GREEN}==============================================================${NC}"
echo -e "${GREEN}Manual curl commands:${NC}"
echo -e "${GREEN}==============================================================${NC}"
echo ""
echo "Query instant value:"
echo "  curl -s '${PROM_URL}/api/v1/query' \\"
echo "    --data-urlencode 'query=YOUR_QUERY' | python3 -m json.tool"
echo ""
echo "Query range (last 5 minutes):"
echo "  curl -s '${PROM_URL}/api/v1/query_range' \\"
echo "    --data-urlencode 'query=YOUR_QUERY' \\"
echo "    --data-urlencode 'start=\$(date -u -v-5M +%s)' \\"
echo "    --data-urlencode 'end=\$(date -u +%s)' \\"
echo "    --data-urlencode 'step=15' | python3 -m json.tool"
echo ""
echo "List all metrics:"
echo "  curl -s '${PROM_URL}/api/v1/label/__name__/values' | python3 -m json.tool"
echo ""
echo "Check targets status:"
echo "  curl -s '${PROM_URL}/api/v1/targets' | python3 -m json.tool"
echo ""
