#!/bin/bash

# ========== 基本参数 ==========
BASE_NAME="Message PC Test"
TOPICS=1
PARTITIONS_PER_TOPIC=16
PRODUCER_RATE=3000
PRODUCERS_PER_TOPIC=1
SUBSCRIPTIONS_PER_TOPIC=1
TEST_DURATION_MINUTES=1
WARMUP_DURATION_MINUTES=0
CONSUMER_BACKLOG_SIZE_GB=0
MESSAGE_PROCESSING_DELAY_MS=5
MSG_SIZE=1024

# ========== 测试参数 ==========
ITERATIONS=1
RESULTS_BASE_DIR="results_pc_$(date +%Y%m%d_%H%M%S)"  # 结果根目录

# ========== Kafka Docker 容器名称 ==========
KAFKA_CONTAINER_NAME="kafka"

# 改变的变量（控制变量实验）
CONSUMER_PER_SUBSCRIPTION=1
MAX_CONSUMER=32

# ========== 项目根目录 ==========
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKLOADS_DIR="${PROJECT_ROOT}/workloads/multi-c-tests"
DRIVER_CONFIG="${PROJECT_ROOT}/examples/kafka-driver.yaml"

# ========== 检查 Kafka 配置提示 ==========
echo "=============================="
echo " Kafka Configuration Check"
echo "=============================="
echo "⚠️  当前测试的最大消息大小为 $((MSG_SIZE / 1024)) KB"
echo "请确保 Kafka 已配置大消息支持："
echo "  - message.max.bytes"
echo "  - replica.fetch.max.bytes"
echo "  - max.request.size (producer)"
echo "  - fetch.max.bytes (consumer)"
echo ""
read -p "是否已经配置完成？(y/n): " confirm
if [[ $confirm != [yY] ]]; then
    echo "请先配置 Kafka 后再运行测试"
    exit 1
fi

# ========== 创建目录 ==========
mkdir -p "$WORKLOADS_DIR"
mkdir -p "$RESULTS_BASE_DIR"
echo "✅ 创建结果目录: $RESULTS_BASE_DIR"
echo "✅ 工作负载目录: $WORKLOADS_DIR"
echo ""

# ========== 主测试循环 ==========
while [ $CONSUMER_PER_SUBSCRIPTION -le $MAX_CONSUMER ]
do
    PC_DIR="${RESULTS_BASE_DIR}/pc_${PRODUCERS_PER_TOPIC}p_${CONSUMER_PER_SUBSCRIPTION}c"
    mkdir -p "$PC_DIR"

    echo "=============================="
    echo " 测试场景: ${PRODUCERS_PER_TOPIC} Producer(s) ↔ ${CONSUMER_PER_SUBSCRIPTION} Consumer(s)"
    echo "=============================="
    echo "结果目录: $PC_DIR"
    echo ""

    for iteration in $(seq 1 $ITERATIONS)
    do
        NAME="${BASE_NAME} - ${PRODUCERS_PER_TOPIC}p-${CONSUMER_PER_SUBSCRIPTION}c - Run ${iteration}"
        echo "------------------------------"
        echo " Run ${iteration}/${ITERATIONS}"
        echo "------------------------------"

        CONFIG_FILE_NAME="pc_${PRODUCERS_PER_TOPIC}p_${CONSUMER_PER_SUBSCRIPTION}c_run${iteration}.yaml"
        CONFIG_FILE="${WORKLOADS_DIR}/${CONFIG_FILE_NAME}"

        cat > "$CONFIG_FILE" <<EOF
name: ${NAME}

topics: ${TOPICS}
partitionsPerTopic: ${PARTITIONS_PER_TOPIC}
messageSize: ${MSG_SIZE}

producersPerTopic: ${PRODUCERS_PER_TOPIC}
producerRate: ${PRODUCER_RATE}
subscriptionsPerTopic: ${SUBSCRIPTIONS_PER_TOPIC}
consumerPerSubscription: ${CONSUMER_PER_SUBSCRIPTION}

messageProcessingDelayMs: ${MESSAGE_PROCESSING_DELAY_MS}

testDurationMinutes: ${TEST_DURATION_MINUTES}
warmupDurationMinutes: ${WARMUP_DURATION_MINUTES}

consumerBacklogSizeGB: ${CONSUMER_BACKLOG_SIZE_GB}
EOF

        echo "✅ 生成配置文件: ${CONFIG_FILE_NAME}"

        OUTPUT_FILE_NAME="result_run${iteration}.json"
        OUTPUT_FILE="$(pwd)/${PC_DIR}/${OUTPUT_FILE_NAME}"
        LOG_FILE="$(pwd)/${PC_DIR}/log_run${iteration}.txt"

        echo "🚀 开始测试..."
        echo "Driver: $DRIVER_CONFIG"
        echo "Workload: $CONFIG_FILE"
        echo "Output: $OUTPUT_FILE"
        echo ""

        (cd "$PROJECT_ROOT" && python -m benchmark -d "$DRIVER_CONFIG" -o "$OUTPUT_FILE" "$CONFIG_FILE") 2>&1 | tee "$LOG_FILE"
        BENCHMARK_EXIT_CODE=$?

        if [ $BENCHMARK_EXIT_CODE -eq 0 ]; then
            echo "✅ Run ${iteration} 完成"
        else
            echo "❌ Run ${iteration} 失败 (退出码: $BENCHMARK_EXIT_CODE)"
            echo "查看日志: $LOG_FILE"
        fi
        echo ""

        if [ $iteration -lt $ITERATIONS ]; then
            echo "⏸ 休息 10 秒..."
            sleep 10
        fi
    done

    echo "🎯 当前轮次完成: ${PRODUCERS_PER_TOPIC}p-${CONSUMER_PER_SUBSCRIPTION}c"
    echo ""
    CONSUMER_PER_SUBSCRIPTION=$((CONSUMER_PER_SUBSCRIPTION * 2))
done

echo "=============================="
echo "✅ 所有测试完成！"
echo "=============================="
echo "结果保存在: $RESULTS_BASE_DIR"
echo ""

# ========== 生成汇总分析脚本 ==========
ANALYSIS_SCRIPT="${RESULTS_BASE_DIR}/analyze_results.py"
cat > "$ANALYSIS_SCRIPT" <<'ANALYSIS_EOF'
#!/usr/bin/env python3
"""
结果分析脚本
统计每个 Producer/Consumer 组合的端到端延迟、速率、吞吐量。
"""
import json
import csv
from pathlib import Path
import statistics

def analyze_results(base_dir):
    results = {}

    for pc_dir in sorted(Path(base_dir).glob("pc_*p_*c")):
        name = pc_dir.name
        latencies = {'50': [], '75': [], '95': [], '99': []}
        latency_avg = []
        producer_rates, consumer_rates = [], []
        producer_throughput, consumer_throughput = [], []

        for result_file in sorted(pc_dir.glob("result_run*.json")):
            with open(result_file, 'r') as f:
                data = json.load(f)
                if 'aggregatedEndToEndLatencyAvg' in data:
                    latency_avg.append(data['aggregatedEndToEndLatencyAvg'])
                for p in latencies.keys():
                    key = f'aggregatedEndToEndLatency{p}pct'
                    if key in data:
                        latencies[p].append(data[key])
                if 'aggregatedPublishRateAvg' in data:
                    producer_rates.append(data['aggregatedPublishRateAvg'])
                if 'aggregatedConsumeRateAvg' in data:
                    consumer_rates.append(data['aggregatedConsumeRateAvg'])
                if 'aggregatedPublishThroughputAvg' in data:
                    producer_throughput.append(data['aggregatedPublishThroughputAvg'])
                if 'aggregatedConsumeThroughputAvg' in data:
                    consumer_throughput.append(data['aggregatedConsumeThroughputAvg'])

        results[name] = {
            'latency_avg': statistics.mean(latency_avg) if latency_avg else None,
            'latency_50': statistics.mean(latencies['50']) if latencies['50'] else None,
            'latency_95': statistics.mean(latencies['95']) if latencies['95'] else None,
            'prod_rate': statistics.mean(producer_rates) if producer_rates else None,
            'cons_rate': statistics.mean(consumer_rates) if consumer_rates else None,
            'prod_thr': statistics.mean(producer_throughput) if producer_throughput else None,
            'cons_thr': statistics.mean(consumer_throughput) if consumer_throughput else None,
        }
    return results

def print_summary(results):
    print("\n" + "="*120)
    print("Kafka Producer/Consumer 性能实验汇总")
    print("="*120)
    header = f"{'场景':<20} {'AvgLat(ms)':<12} {'P50(ms)':<10} {'P95(ms)':<10} {'ProdRate':<12} {'ConsRate':<12} {'ProdThr(MB/s)':<15} {'ConsThr(MB/s)':<15}"
    print(header)
    print("-"*120)

    for name, data in results.items():
        row = f"{name:<20}"
        row += f"{data['latency_avg'] or 0:.2f}".ljust(12)
        row += f"{data['latency_50'] or 0:.2f}".ljust(10)
        row += f"{data['latency_95'] or 0:.2f}".ljust(10)
        row += f"{data['prod_rate'] or 0:.1f}".ljust(12)
        row += f"{data['cons_rate'] or 0:.1f}".ljust(12)
        row += f"{data['prod_thr'] or 0:.2f}".ljust(15)
        row += f"{data['cons_thr'] or 0:.2f}".ljust(15)
        print(row)
    print("="*120)

def save_to_csv(results, output_file):
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Scenario','AvgLat(ms)','P50(ms)','P95(ms)',
                         'ProdRate','ConsRate','ProdThr(MB/s)','ConsThr(MB/s)'])
        for name, data in results.items():
            writer.writerow([
                name, data['latency_avg'], data['latency_50'], data['latency_95'],
                data['prod_rate'], data['cons_rate'], data['prod_thr'], data['cons_thr']
            ])
    print(f"✅ 汇总结果已保存到: {output_file}")

if __name__ == '__main__':
    base_dir = Path(__file__).parent
    results = analyze_results(base_dir)
    print_summary(results)
    save_to_csv(results, base_dir / 'summary.csv')

ANALYSIS_EOF

chmod +x "$ANALYSIS_SCRIPT"
echo "=============================="
echo "📊 分析脚本已生成"
echo "=============================="
echo "运行以下命令查看结果汇总："
echo "  python3 ${ANALYSIS_SCRIPT}"
echo ""
