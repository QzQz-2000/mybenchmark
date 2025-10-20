#!/bin/bash

# =====================================================
# Kafka 吞吐量快速检查脚本
# =====================================================

CONTAINER_NAME="kafka101"
TOPIC_NAME="benchmark-topic-0"  # 根据你的测试 topic 名称调整

echo "=========================================="
echo "  Kafka 吞吐量检查工具"
echo "=========================================="
echo ""

# 检查容器是否运行
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "错误：容器 ${CONTAINER_NAME} 未运行"
    exit 1
fi

echo "1. 检查所有 topic 列表："
docker exec ${CONTAINER_NAME} kafka-topics --bootstrap-server localhost:9092 --list
echo ""

echo "2. 请输入要检查的 topic 名称（或直接回车使用默认: ${TOPIC_NAME}）："
read -r USER_TOPIC
if [ -n "$USER_TOPIC" ]; then
    TOPIC_NAME="$USER_TOPIC"
fi

echo ""
echo "3. 查看 topic '${TOPIC_NAME}' 的详细信息："
docker exec ${CONTAINER_NAME} kafka-topics --bootstrap-server localhost:9092 --describe --topic ${TOPIC_NAME}
echo ""

echo "4. 查看 topic '${TOPIC_NAME}' 的消息数量（检查各分区的 offset）："
docker exec ${CONTAINER_NAME} kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic ${TOPIC_NAME} \
    --time -1
echo ""

echo "5. 实时监控消息生产速率（按 Ctrl+C 停止）："
echo "   提示：观察 LEO (Log End Offset) 的增长速度"
echo ""

# 循环监控
PREV_OFFSET=0
while true; do
    # 获取当前最大 offset
    CURRENT_OFFSET=$(docker exec ${CONTAINER_NAME} kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 \
        --topic ${TOPIC_NAME} \
        --time -1 2>/dev/null | awk -F':' '{sum+=$3} END {print sum}')

    if [ -n "$CURRENT_OFFSET" ] && [ "$CURRENT_OFFSET" -gt 0 ]; then
        if [ $PREV_OFFSET -gt 0 ]; then
            DELTA=$((CURRENT_OFFSET - PREV_OFFSET))
            echo "$(date '+%H:%M:%S') - 总消息数: $CURRENT_OFFSET (+$DELTA/5s = $((DELTA/5)) msg/s)"
        else
            echo "$(date '+%H:%M:%S') - 总消息数: $CURRENT_OFFSET"
        fi
        PREV_OFFSET=$CURRENT_OFFSET
    fi

    sleep 5
done
