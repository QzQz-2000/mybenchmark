#!/bin/bash

echo "=========================================="
echo "📊 高吞吐量Consumer性能测试 - 结果分析"
echo "=========================================="
echo ""

FILE1="workload-ht-5-Kafka-2025-10-09-03-54-18.json"
FILE2="workload-ht-10-Kafka-2025-10-09-04-09-06.json"
FILE3="workload-ht-20-Kafka-2025-10-09-04-24-01.json"

echo "测试配置："
echo "  - 分区数: 10"
echo "  - Producer数: 50 @ 100 msg/s = 目标5000 msg/s"
echo "  - 消息大小: 1024 bytes"
echo ""

# 提取并计算平均值
get_avg() {
    jq -r ".$1 | add / length" "$2" 2>/dev/null | awk '{printf "%.1f", $1}'
}

get_val() {
    jq -r ".$1" "$2" 2>/dev/null | awk '{printf "%.2f", $1}'
}

# 1. 吞吐量对比
echo "=========================================="
echo "🎯 吞吐量对比 (关键指标！)"
echo "=========================================="
printf "%-25s | %15s | %15s | %15s\n" "指标" "Consumer=5" "Consumer=10" "Consumer=20"
echo "--------------------------------------------------------------------------------"

prod1=$(get_avg "publishRate" "$FILE1")
prod2=$(get_avg "publishRate" "$FILE2")
prod3=$(get_avg "publishRate" "$FILE3")
printf "%-25s | %13s/s | %13s/s | %13s/s\n" "生产速率(msg)" "$prod1" "$prod2" "$prod3"

cons1=$(get_avg "consumeRate" "$FILE1")
cons2=$(get_avg "consumeRate" "$FILE2")
cons3=$(get_avg "consumeRate" "$FILE3")
printf "%-25s | %13s/s | %13s/s | %13s/s\n" "消费速率(msg)" "$cons1" "$cons2" "$cons3"

# 计算跟进度
if [ "$prod1" != "0" ] && [ "$prod1" != "" ]; then
    pct1=$(echo "scale=1; $cons1 * 100 / $prod1" | bc 2>/dev/null || echo "N/A")
    pct2=$(echo "scale=1; $cons2 * 100 / $prod2" | bc 2>/dev/null || echo "N/A")
    pct3=$(echo "scale=1; $cons3 * 100 / $prod3" | bc 2>/dev/null || echo "N/A")
    printf "%-25s | %14s%% | %14s%% | %14s%%\n" "消费跟进度" "$pct1" "$pct2" "$pct3"
fi

echo ""
echo "=========================================="
echo "⏱  延迟对比"
echo "=========================================="

lat50_1=$(get_val "aggregatedPublishLatency50pct" "$FILE1")
lat50_2=$(get_val "aggregatedPublishLatency50pct" "$FILE2")
lat50_3=$(get_val "aggregatedPublishLatency50pct" "$FILE3")
printf "%-25s | %13s ms | %13s ms | %13s ms\n" "Pub Latency P50" "$lat50_1" "$lat50_2" "$lat50_3"

lat99_1=$(get_val "aggregatedPublishLatency99pct" "$FILE1")
lat99_2=$(get_val "aggregatedPublishLatency99pct" "$FILE2")
lat99_3=$(get_val "aggregatedPublishLatency99pct" "$FILE3")
printf "%-25s | %13s ms | %13s ms | %13s ms\n" "Pub Latency P99" "$lat99_1" "$lat99_2" "$lat99_3"

latmax_1=$(get_val "aggregatedPublishLatencyMax" "$FILE1")
latmax_2=$(get_val "aggregatedPublishLatencyMax" "$FILE2")
latmax_3=$(get_val "aggregatedPublishLatencyMax" "$FILE3")
printf "%-25s | %13s ms | %13s ms | %13s ms\n" "Pub Latency Max" "$latmax_1" "$latmax_2" "$latmax_3"

echo ""
echo "=========================================="
echo "🔍 详细分析"
echo "=========================================="
echo ""

# 对比消费速率
echo "【吞吐量分析】"
echo "  目标生产速率: 5000 msg/s"
echo "  实际生产速率: Consumer=5:$prod1, Consumer=10:$prod2, Consumer=20:$prod3"
echo ""

if (( $(echo "$cons1 < 4500" | bc -l 2>/dev/null || echo 0) )); then
    echo "  ❌ Consumer=5: 消费速率 $cons1 msg/s < 4500 - 明显跟不上！"
else
    echo "  ✅ Consumer=5: 消费速率 $cons1 msg/s - 基本跟上"
fi

if (( $(echo "$cons2 >= 4500" | bc -l 2>/dev/null || echo 0) )); then
    echo "  ✅ Consumer=10: 消费速率 $cons2 msg/s - 完全跟上"
else
    echo "  ⚠️  Consumer=10: 消费速率 $cons2 msg/s - 有点吃力"
fi

if (( $(echo "$cons3 >= 4500" | bc -l 2>/dev/null || echo 0) )); then
    echo "  ✅ Consumer=20: 消费速率 $cons3 msg/s - 轻松跟上（但资源浪费）"
else
    echo "  ⚠️  Consumer=20: 消费速率 $cons3 msg/s"
fi

echo ""
echo "【延迟分析】"
echo "  对比P99延迟（越低越好）："
echo "    Consumer=5:  $lat99_1 ms"
echo "    Consumer=10: $lat99_2 ms"
echo "    Consumer=20: $lat99_3 ms"

echo ""
echo "=========================================="
echo "💡 结论"
echo "=========================================="
echo ""
echo "在5000 msg/s高吞吐场景下："
echo ""
echo "  🏆 最佳配置: Consumer=10"
echo "     理由: 吞吐量跟上 + 资源利用合理 + 延迟适中"
echo ""
echo "  ⚠️  Consumer=5: "
if (( $(echo "$cons1 < 4500" | bc -l 2>/dev/null || echo 0) )); then
    echo "     问题: 消费能力不足，会产生积压"
else
    echo "     可用: 勉强跟上，但无冗余"
fi
echo ""
echo "  💰 Consumer=20: "
echo "     问题: 10个consumer闲置，资源浪费50%"
echo ""
