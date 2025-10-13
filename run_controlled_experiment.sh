#!/bin/bash
# 控制变量实验：固定吞吐量约2-3 MB/s
# 对比不同消息大小对延迟的影响

set -e

DRIVER="examples/kafka-driver.yaml"
RESULTS_DIR="./results-controlled"
TIMESTAMP=$(date +"%Y-%m-%d-%H-%M-%S")

mkdir -p "$RESULTS_DIR"

echo "======================================"
echo "控制变量实验：固定吞吐量 ~2.5 MB/s"
echo "测试不同消息大小对延迟的影响"
echo "开始时间: $(date)"
echo "======================================"
echo ""

# 测试配置数组
# 格式: "消息大小:Agents数:每Agent速率:预期吞吐量"
declare -a TESTS=(
    "256:10:200:0.49"      # 256B × 2000 msg/s = 0.49 MB/s
    "1024:10:100:0.98"     # 1KB × 1000 msg/s = 0.98 MB/s
    "4096:10:65:2.53"      # 4KB × 650 msg/s = 2.53 MB/s
    "16384:10:16:2.50"     # 16KB × 160 msg/s = 2.50 MB/s
    "65536:10:4:2.50"      # 64KB × 40 msg/s = 2.50 MB/s
)

for TEST in "${TESTS[@]}"; do
    IFS=':' read -r SIZE AGENTS RATE THROUGHPUT <<< "$TEST"

    SIZE_KB=$((SIZE / 1024))
    if [ $SIZE_KB -eq 0 ]; then
        SIZE_LABEL="${SIZE}B"
    else
        SIZE_LABEL="${SIZE_KB}KB"
    fi

    echo "========================================"
    echo "测试: 消息大小 = $SIZE_LABEL"
    echo "配置: $AGENTS Agents × $RATE msg/s"
    echo "预期吞吐量: $THROUGHPUT MB/s"
    echo "========================================"

    # 生成配置
    cat > /tmp/test-workload.yaml << EOF
name: msg-size-${SIZE}B-controlled
topics: 1
partitionsPerTopic: 10
messageSize: ${SIZE}
keyDistributor: RANDOM_NANO
subscriptionsPerTopic: 1
producersPerTopic: ${AGENTS}
consumerPerSubscription: 10
producerRate: ${RATE}
testDurationMinutes: 2
warmupDurationMinutes: 0
EOF

    echo "开始测试..."
    python -m benchmark.benchmark \
        -d "$DRIVER" \
        /tmp/test-workload.yaml 2>&1 | tee "$RESULTS_DIR/log-${SIZE}B-${TIMESTAMP}.txt"

    echo ""
    echo "测试完成，等待10秒..."
    sleep 10
    echo ""
done

echo "======================================"
echo "所有测试完成！"
echo "结束时间: $(date)"
echo "======================================"
echo ""
echo "结果文件位置:"
echo "  - 日志: $RESULTS_DIR/log-*"
echo "  - JSON: msg-size-*-controlled-Kafka-*.json"
echo ""
echo "重命名结果文件以便分析:"
echo "  bash rename_controlled_results.sh"
