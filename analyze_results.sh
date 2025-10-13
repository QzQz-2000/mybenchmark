#!/bin/bash
echo "=========================================="
echo "📊 Consumer数量实验结果分析"
echo "=========================================="
echo ""

# 提取关键指标
extract_metric() {
    file=$1
    metric=$2
    jq -r ".$metric // \"N/A\"" "$file" 2>/dev/null
}

# 文件路径
FILE1="workload-consumer-test-under-Kafka-2025-10-09-02-46-16.json"
FILE2="workload-consumer-test-optimal-Kafka-2025-10-09-02-52-29.json"
FILE3="workload-consumer-test-over-Kafka-2025-10-09-02-58-41.json"

echo "测试配置："
echo "  - 分区数: 10"
echo "  - Producer数: 10 @ 10 msg/s = 100 msg/s总吞吐"
echo "  - 消息大小: 1024 bytes"
echo ""

# 打印表头
printf "%-30s | %12s | %12s | %12s\n" "指标" "Consumer=5" "Consumer=10" "Consumer=20"
printf "%s\n" "--------------------------------------------------------------------------------------"

# 吞吐量指标
echo "【吞吐量指标】"
rate1=$(extract_metric "$FILE1" "publishRate" | jq -r 'add / length' 2>/dev/null | awk '{printf "%.1f", $1}')
rate2=$(extract_metric "$FILE2" "publishRate" | jq -r 'add / length' 2>/dev/null | awk '{printf "%.1f", $1}')
rate3=$(extract_metric "$FILE3" "publishRate" | jq -r 'add / length' 2>/dev/null | awk '{printf "%.1f", $1}')
printf "%-30s | %10s/s | %10s/s | %10s/s\n" "生产速率(msg)" "$rate1" "$rate2" "$rate3"

cons1=$(extract_metric "$FILE1" "consumeRate" | jq -r 'add / length' 2>/dev/null | awk '{printf "%.1f", $1}')
cons2=$(extract_metric "$FILE2" "consumeRate" | jq -r 'add / length' 2>/dev/null | awk '{printf "%.1f", $1}')
cons3=$(extract_metric "$FILE3" "consumeRate" | jq -r 'add / length' 2>/dev/null | awk '{printf "%.1f", $1}')
printf "%-30s | %10s/s | %10s/s | %10s/s\n" "消费速率(msg)" "$cons1" "$cons2" "$cons3"

echo ""
echo "【延迟指标 - 发布延迟】"
# 发布延迟P50
lat50_1=$(extract_metric "$FILE1" "aggregatedPublishLatency50pct" | awk '{printf "%.2f", $1}')
lat50_2=$(extract_metric "$FILE2" "aggregatedPublishLatency50pct" | awk '{printf "%.2f", $1}')
lat50_3=$(extract_metric "$FILE3" "aggregatedPublishLatency50pct" | awk '{printf "%.2f", $1}')
printf "%-30s | %10s ms | %10s ms | %10s ms\n" "Pub Latency P50" "$lat50_1" "$lat50_2" "$lat50_3"

# 发布延迟P99
lat99_1=$(extract_metric "$FILE1" "aggregatedPublishLatency99pct" | awk '{printf "%.2f", $1}')
lat99_2=$(extract_metric "$FILE2" "aggregatedPublishLatency99pct" | awk '{printf "%.2f", $1}')
lat99_3=$(extract_metric "$FILE3" "aggregatedPublishLatency99pct" | awk '{printf "%.2f", $1}')
printf "%-30s | %10s ms | %10s ms | %10s ms\n" "Pub Latency P99" "$lat99_1" "$lat99_2" "$lat99_3"

# 发布延迟Max
latmax_1=$(extract_metric "$FILE1" "aggregatedPublishLatencyMax" | awk '{printf "%.2f", $1}')
latmax_2=$(extract_metric "$FILE2" "aggregatedPublishLatencyMax" | awk '{printf "%.2f", $1}')
latmax_3=$(extract_metric "$FILE3" "aggregatedPublishLatencyMax" | awk '{printf "%.2f", $1}')
printf "%-30s | %10s ms | %10s ms | %10s ms\n" "Pub Latency Max" "$latmax_1" "$latmax_2" "$latmax_3"

echo ""
echo "【延迟指标 - Pub Delay (调度精度)】"
# Pub Delay P99
delay99_1=$(extract_metric "$FILE1" "aggregatedPublishDelayLatency99pct" | awk '{printf "%.0f", $1}')
delay99_2=$(extract_metric "$FILE2" "aggregatedPublishDelayLatency99pct" | awk '{printf "%.0f", $1}')
delay99_3=$(extract_metric "$FILE3" "aggregatedPublishDelayLatency99pct" | awk '{printf "%.0f", $1}')
printf "%-30s | %10s µs | %10s µs | %10s µs\n" "Pub Delay P99" "$delay99_1" "$delay99_2" "$delay99_3"

echo ""
echo "=========================================="
echo "🔍 分析结论："
echo "=========================================="
echo ""
echo "✅ 预期验证："
echo "  1. 吞吐量：三种配置都应该接近100 msg/s（Producer限速）"
echo "  2. Pub Latency：主要受Kafka ACK影响，三者应该相近"
echo "  3. Consumer数量主要影响消费端负载分布，不影响生产端性能"
echo ""
echo "🎯 最佳配置："
echo "  - Consumer=10（1:1映射）是最优配置"
echo "  - Consumer=5 每个consumer处理2个分区，负载加倍"
echo "  - Consumer=20 有10个consumer闲置，浪费资源"
echo ""
