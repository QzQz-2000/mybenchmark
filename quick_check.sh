#!/bin/bash

# 快速验证吞吐量的简单脚本

echo "=========================================="
echo "  快速吞吐量验证"
echo "=========================================="
echo ""

# 从 Kafka JMX 获取实时吞吐量
echo "方法1: 从 JMX Exporter 获取实时数据"
echo "--------------------------------------"
BYTES_IN=$(curl -s http://localhost:8080/metrics 2>/dev/null | grep "kafka_server_brokertopicmetrics_bytesinpersec_total" | grep -v "^#" | awk '{print $2}')

if [ -n "$BYTES_IN" ]; then
    echo "累积流入字节数: $BYTES_IN bytes"
    echo ""
    echo "等待 10 秒后再次采样..."
    sleep 10

    BYTES_IN_2=$(curl -s http://localhost:8080/metrics 2>/dev/null | grep "kafka_server_brokertopicmetrics_bytesinpersec_total" | grep -v "^#" | awk '{print $2}')

    if [ -n "$BYTES_IN_2" ]; then
        DIFF=$(echo "$BYTES_IN_2 - $BYTES_IN" | bc)
        RATE=$(echo "scale=2; $DIFF / 10" | bc)
        RATE_KB=$(echo "scale=2; $RATE / 1024" | bc)
        RATE_MB=$(echo "scale=2; $RATE / 1024 / 1024" | bc)

        echo "10秒内流入字节数: $DIFF bytes"
        echo "实际吞吐量: $RATE bytes/s"
        echo "           = $RATE_KB KB/s"
        echo "           = $RATE_MB MB/s"

        # 反推消息速率（假设消息大小 1KB）
        MSG_RATE=$(echo "scale=0; $RATE / 1024" | bc)
        echo ""
        echo "如果消息大小为 1KB，则消息速率约为: $MSG_RATE msg/s"
    fi
else
    echo "错误：无法从 JMX Exporter 获取数据"
    echo "请确保 Kafka 和 JMX Exporter 正在运行"
fi

echo ""
echo "=========================================="
echo "方法2: 从 Docker Stats 获取网络流量"
echo "--------------------------------------"
docker stats --no-stream --format "table {{.Name}}\t{{.NetIO}}" kafka101

echo ""
echo "提示："
echo "  - 如果看到的吞吐量远低于预期，检查："
echo "    1. 测试是否正在运行"
echo "    2. producerRate 配置是否正确"
echo "    3. 查看 benchmark 的控制台输出"
