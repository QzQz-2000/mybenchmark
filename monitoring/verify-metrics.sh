#!/bin/bash

# Kafka 监控指标验证脚本
# 检查 Dashboard 中使用的所有指标是否可用

echo "========================================="
echo "Kafka 监控指标验证"
echo "========================================="
echo ""

PROMETHEUS_URL="http://localhost:9090"
JMX_URL="http://localhost:5556"

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查指标函数
check_metric() {
    local metric=$1
    local name=$2

    # 查询 Prometheus
    result=$(curl -s "${PROMETHEUS_URL}/api/v1/query?query=${metric}" | grep -o '"status":"success"')

    if [ -n "$result" ]; then
        echo -e "${GREEN}✓${NC} ${name}"
        echo "  查询: ${metric}"
        return 0
    else
        echo -e "${RED}✗${NC} ${name}"
        echo "  查询: ${metric}"
        return 1
    fi
}

echo "1. 吞吐量指标"
echo "-------------------"
check_metric "kafka_broker_topic_messagesinpersec_total" "消息流入总数"
check_metric "kafka_broker_topic_bytesinpersec_total" "字节流入总数"
check_metric "kafka_broker_topic_bytesoutpersec_total" "字节流出总数"
echo ""

echo "2. 延迟指标"
echo "-------------------"
check_metric "kafka_network_request_totaltimems_percentile{request=\"Produce\",percentile=\"99\"}" "Produce p99 延迟"
check_metric "kafka_network_request_totaltimems_percentile{request=\"FetchConsumer\",percentile=\"99\"}" "Fetch p99 延迟"
echo ""

echo "3. Broker 状态指标"
echo "-------------------"
check_metric "kafka_server_kafkaserver_brokerstate" "Broker 状态"
check_metric "kafka_controller_kafkacontroller_activebrokercount" "活跃 Broker 数量"
check_metric "kafka_server_replicamanager_partitioncount" "分区数量"
check_metric "kafka_server_replicamanager_underreplicatedpartitions" "副本不足的分区"
echo ""

echo "4. JVM 指标"
echo "-------------------"
check_metric "jvm_memory_heap_used" "JVM 堆内存使用"
check_metric "jvm_memory_heap_committed" "JVM 堆内存已提交"
check_metric "jvm_memory_heap_max" "JVM 堆内存最大值"
check_metric "jvm_gc_collection_count" "GC 次数"
echo ""

echo "5. 网络和请求指标"
echo "-------------------"
check_metric "kafka_network_request_totaltimems_total{request=\"Produce\"}" "Produce 请求总数"
check_metric "kafka_network_request_totaltimems_total{request=\"FetchConsumer\"}" "Fetch 请求总数"
check_metric "kafka_network_processor_idlepercent" "网络处理器空闲率"
check_metric "kafka_network_requestchannel_requestqueuesize" "请求队列大小"
check_metric "kafka_network_requestchannel_responsequeuesize" "响应队列大小"
echo ""

echo "6. 磁盘 I/O 指标"
echo "-------------------"
check_metric "kafka_server_kafkaserver_linux_disk_read_bytes" "磁盘读取字节"
check_metric "kafka_server_kafkaserver_linux_disk_write_bytes" "磁盘写入字节"
echo ""

echo "========================================="
echo "检查 JMX Exporter 端点"
echo "========================================="
if curl -s "${JMX_URL}/metrics" > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} JMX Exporter 正常运行 (${JMX_URL})"
    metric_count=$(curl -s "${JMX_URL}/metrics" | grep "^kafka_" | wc -l)
    echo "  导出的 Kafka 指标数量: ${metric_count}"
else
    echo -e "${RED}✗${NC} JMX Exporter 无法访问"
fi
echo ""

echo "========================================="
echo "检查 Prometheus Targets"
echo "========================================="
targets_status=$(curl -s "${PROMETHEUS_URL}/api/v1/targets" | grep -o '"health":"up"' | wc -l)
echo "健康的 Targets 数量: ${targets_status}"
echo ""

echo "========================================="
echo "验证完成！"
echo "========================================="
echo ""
echo "访问地址:"
echo "  - Grafana: http://localhost:3000"
echo "  - Prometheus: http://localhost:9090"
echo "  - JMX Exporter: http://localhost:5556/metrics"
echo ""
