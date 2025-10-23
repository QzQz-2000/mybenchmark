#!/bin/bash

# ========== 基本参数 ==========
BASE_NAME="Message Size Test"
TOPICS=1
PARTITIONS_PER_TOPIC=1
PRODUCERS_PER_TOPIC=1
PRODUCER_RATE=200000
SUBSCRIPTIONS_PER_TOPIC=1
CONSUMER_PER_SUBSCRIPTION=1
TEST_DURATION_MINUTES=1
WARMUP_DURATION_MINUTES=0
CONSUMER_BACKLOG_SIZE_GB=0

# ========== 测试参数 ==========
ITERATIONS=1        # 每个消息大小运行1次（根据您最新的输入修改）
RESULTS_BASE_DIR="results_$(date +%Y%m%d_%H%M%S)"  # 结果根目录

# ========== Kafka Docker 容器名称 ==========
KAFKA_CONTAINER_NAME="kafka"  # 请根据实际容器名称修改

# ========== 起始与最大大小（字节）==========
SIZE=1024             # 起始大小 8B
MAX_SIZE=$((128 * 1024 * 1024))  # 128MB

# ========== 项目根目录 ==========
# 脚本在 tests 目录中，项目根目录在上一级
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKLOADS_DIR="${PROJECT_ROOT}/workloads/msg-size-tests"
DRIVER_CONFIG="${PROJECT_ROOT}/examples/kafka-driver.yaml"

# ========== 检查 Kafka 配置提示 ==========
echo "=============================="
echo " Kafka Configuration Check"
echo "=============================="
echo "⚠️  重要提示：测试最大消息大小为 $((MAX_SIZE / 1024))KB"
echo "如果 MAX_SIZE 很大，请确保 Kafka 配置支持大消息传输，需要设置："
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

# ========== 创建 workloads 目录和结果根目录 ==========
mkdir -p "$WORKLOADS_DIR"
mkdir -p "$RESULTS_BASE_DIR"
echo "✅ 创建结果目录: $RESULTS_BASE_DIR"
echo "✅ 工作负载目录: $WORKLOADS_DIR"
echo ""


while [ $SIZE -le $MAX_SIZE ]
do
    SIZE_DIR="${RESULTS_BASE_DIR}/size_${SIZE}B"
    mkdir -p "$SIZE_DIR"

    echo "=============================="
    echo " Testing Message Size: ${SIZE}B"
    echo "=============================="

    # === 🧠 动态限速 ===
    if [ $SIZE -le 1024 ]; then
        PRODUCER_RATE_DYNAMIC=10000
    elif [ $SIZE -le $((64 * 1024)) ]; then
        PRODUCER_RATE_DYNAMIC=5000
    elif [ $SIZE -le $((1 * 1024 * 1024)) ]; then
        PRODUCER_RATE_DYNAMIC=5000
    elif [ $SIZE -le $((16 * 1024 * 1024)) ]; then
        PRODUCER_RATE_DYNAMIC=500
    else
        PRODUCER_RATE_DYNAMIC=100
    fi
    echo "🧠 动态限速: messageSize=${SIZE}B, producerRate=${PRODUCER_RATE_DYNAMIC}"

    for iteration in $(seq 1 $ITERATIONS)
    do
        NAME="${BASE_NAME} - ${SIZE}B - Run ${iteration}"

        CONFIG_FILE_NAME="msg_size_${SIZE}B_run${iteration}.yaml"
        CONFIG_FILE="${WORKLOADS_DIR}/${CONFIG_FILE_NAME}"

        cat > $CONFIG_FILE <<EOF
name: ${NAME}

topics: ${TOPICS}
partitionsPerTopic: ${PARTITIONS_PER_TOPIC}
messageSize: ${SIZE}

producersPerTopic: ${PRODUCERS_PER_TOPIC}
producerRate: ${PRODUCER_RATE_DYNAMIC}
subscriptionsPerTopic: ${SUBSCRIPTIONS_PER_TOPIC}
consumerPerSubscription: ${CONSUMER_PER_SUBSCRIPTION}

testDurationMinutes: ${TEST_DURATION_MINUTES}
warmupDurationMinutes: ${WARMUP_DURATION_MINUTES}

consumerBacklogSizeGB: ${CONSUMER_BACKLOG_SIZE_GB}
EOF


        echo "✅ 生成配置文件: ${CONFIG_FILE_NAME}"

        # 准备输出文件
        # JSON 结果保存在每个 size 的子目录中
        OUTPUT_FILE_NAME="result_run${iteration}.json"
        OUTPUT_FILE_RELATIVE="$(pwd)/${SIZE_DIR}/${OUTPUT_FILE_NAME}"
        MONITOR_LOG="$(pwd)/${SIZE_DIR}/monitor_run${iteration}.csv"
        LOG_FILE="$(pwd)/${SIZE_DIR}/log_run${iteration}.txt"


        # ========== 运行 OpenMessaging Benchmark ==========
        echo "🚀 开始测试..."
        echo "Driver: $DRIVER_CONFIG"
        echo "Workload: $CONFIG_FILE"
        echo "Output: $OUTPUT_FILE_RELATIVE"
        echo ""

        # 在项目根目录运行 benchmark
        (cd "$PROJECT_ROOT" && python -m benchmark -d "$DRIVER_CONFIG" -o "$OUTPUT_FILE_RELATIVE" "$CONFIG_FILE") 2>&1 | tee "$LOG_FILE"
        BENCHMARK_EXIT_CODE=$?


        if [ $BENCHMARK_EXIT_CODE -eq 0 ]; then
            echo "✅ Run ${iteration} 完成"
        else
            echo "❌ Run ${iteration} 失败 (退出码: $BENCHMARK_EXIT_CODE)"
            echo "查看日志: $LOG_FILE"
        fi
        echo ""

        # 在迭代之间短暂休息，让系统恢复
        if [ $iteration -lt $ITERATIONS ]; then
            echo "⏸  休息 10 秒..."
            sleep 10
        fi
    done

    echo "✅ 消息大小 ${SIZE}B 的所有测试完成"
    echo ""

    # ========== 下一轮（乘 4）==========
    SIZE=$((SIZE * 2))
done

echo "=============================="
echo "✅ 所有测试完成！"
echo "=============================="
echo "结果保存在: $RESULTS_BASE_DIR"
echo ""

# ========== 生成汇总分析脚本（更新版） ==========
ANALYSIS_SCRIPT="${RESULTS_BASE_DIR}/analyze_results.py"
cat > "$ANALYSIS_SCRIPT" <<'ANALYSIS_EOF'
#!/usr/bin/env python3
"""
结果分析脚本（更新版）
统计每个消息大小的 End-to-End 延迟（平均值和各分位）、Producer/Consumer 速率、Producer/Consumer 吞吐量
"""

import json
import csv
from pathlib import Path
import statistics

def analyze_results(base_dir):
    results = {}

    for size_dir in sorted(Path(base_dir).glob("size_*")):
        size_name = size_dir.name
        message_size = size_name.replace("size_", "").replace("B", "")

        latencies = {'50': [], '75': [], '95': [], '99': []}
        latency_avg = [] # 新增：用于存储 End-to-End 平均延迟
        producer_rates = []
        consumer_rates = []
        producer_throughput = []
        consumer_throughput = []


        # 遍历 JSON 文件
        for result_file in sorted(size_dir.glob("result_run*.json")):
            with open(result_file, 'r') as f:
                data = json.load(f)

                # 提取 End-to-End 平均延迟
                if 'aggregatedEndToEndLatencyAvg' in data:
                    latency_avg.append(data['aggregatedEndToEndLatencyAvg'])
                    
                # 提取 End-to-End 百分位延迟
                for p in latencies.keys():
                    key = f'aggregatedEndToEndLatency{p}pct'
                    if key in data:
                        latencies[p].append(data[key])
                        
                # 提取速率和吞吐量
                if 'aggregatedPublishRateAvg' in data:
                    producer_rates.append(data['aggregatedPublishRateAvg'])
                if 'aggregatedConsumeRateAvg' in data:
                    consumer_rates.append(data['aggregatedConsumeRateAvg'])
                if 'aggregatedPublishThroughputAvg' in data:
                    producer_throughput.append(data['aggregatedPublishThroughputAvg'])
                if 'aggregatedConsumeThroughputAvg' in data:
                    consumer_throughput.append(data['aggregatedConsumeThroughputAvg'])
        

        # 统计平均值和标准差
        results[message_size] = {}
        # 迭代次数 (用于CSV)
        results[message_size]['iterations'] = len(producer_rates) 

        # 统计平均延迟
        results[message_size]['latency_avg_avg'] = statistics.mean(latency_avg) if latency_avg else None
        results[message_size]['latency_avg_std'] = statistics.stdev(latency_avg) if len(latency_avg) > 1 else 0
        
        # 统计百分位延迟
        for p in latencies.keys():
            vals = latencies[p]
            results[message_size][f'latency_{p}_avg'] = statistics.mean(vals) if vals else None
            results[message_size][f'latency_{p}_std'] = statistics.stdev(vals) if len(vals) > 1 else 0

        results[message_size]['producer_rate_avg'] = statistics.mean(producer_rates) if producer_rates else None
        results[message_size]['producer_rate_std'] = statistics.stdev(producer_rates) if len(producer_rates) > 1 else 0
        results[message_size]['consumer_rate_avg'] = statistics.mean(consumer_rates) if consumer_rates else None
        results[message_size]['consumer_rate_std'] = statistics.stdev(consumer_rates) if len(consumer_rates) > 1 else 0

        results[message_size]['producer_throughput_avg'] = statistics.mean(producer_throughput) if producer_throughput else None
        results[message_size]['producer_throughput_std'] = statistics.stdev(producer_throughput) if len(producer_throughput) > 1 else 0
        results[message_size]['consumer_throughput_avg'] = statistics.mean(consumer_throughput) if consumer_throughput else None
        results[message_size]['consumer_throughput_std'] = statistics.stdev(consumer_throughput) if len(consumer_throughput) > 1 else 0

    return results

def print_summary(results):
    print("\n" + "="*160)
    print("测试结果汇总 - 消息大小扩展性测试")
    print("="*160)
    # 头部增加 LatencyAvg(ms)
    header = f"{'消息大小':<10} {'LatencyAvg(ms)':<18} {'Latency50(ms)':<18} {'Latency75(ms)':<18} {'Latency95(ms)':<18} {'Latency99(ms)':<18} {'ProdRate':<12} {'ConsRate':<12} {'ProdThr(MB/s)':<15} {'ConsThr(MB/s)':<15}"
    print(header)
    print("-"*160)

    for size in sorted(results.keys(), key=lambda x: int(x)):
        data = results[size]
        row = f"{size+'B':<10} "
        
        # 依次输出 Avg, 50, 75, 95, 99 延迟
        for p_key in ['avg', '50','75','95','99']:
            avg = data[f'latency_{p_key}_avg']
            std = data[f'latency_{p_key}_std']
            
            row += f"{avg:.2f}±{std:.2f}" if avg is not None else "N/A"
            row += " " * (18 - len(f"{avg:.2f}±{std:.2f}"))
            
        row += f"{data['producer_rate_avg']:.1f}±{data['producer_rate_std']:.1f}".ljust(12)
        row += f"{data['consumer_rate_avg']:.1f}±{data['consumer_rate_std']:.1f}".ljust(12)
        row += f"{data['producer_throughput_avg']:.2f}±{data['producer_throughput_std']:.2f}".ljust(15)
        row += f"{data['consumer_throughput_avg']:.2f}±{data['consumer_throughput_std']:.2f}".ljust(15)
        print(row)
    print("="*160)

def save_to_csv(results, output_file):
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        # 头部增加 LatencyAvg_Avg(ms) 和 LatencyAvg_Std(ms)
        header = ['MessageSize(B)',
                  'LatencyAvg_Avg(ms)','LatencyAvg_Std(ms)',
                  'Latency50_Avg(ms)','Latency50_Std(ms)',
                  'Latency75_Avg(ms)','Latency75_Std(ms)',
                  'Latency95_Avg(ms)','Latency95_Std(ms)',
                  'Latency99_Avg(ms)','Latency99_Std(ms)',
                  'ProducerRate_Avg','ProducerRate_Std',
                  'ConsumerRate_Avg','ConsumerRate_Std',
                  'ProducerThroughput_Avg(MB/s)','ProducerThroughput_Std(MB/s)',
                  'ConsumerThroughput_Avg(MB/s)','ConsumerThroughput_Std(MB/s)',
                  'Iterations']
        writer.writerow(header)

        for size in sorted(results.keys(), key=lambda x: int(x)):
            data = results[size]
            writer.writerow([
                size,
                data['latency_avg_avg'], data['latency_avg_std'],
                data['latency_50_avg'], data['latency_50_std'],
                data['latency_75_avg'], data['latency_75_std'],
                data['latency_95_avg'], data['latency_95_std'],
                data['latency_99_avg'], data['latency_99_std'],
                data['producer_rate_avg'], data['producer_rate_std'],
                data['consumer_rate_avg'], data['consumer_rate_std'],
                data['producer_throughput_avg'], data['producer_throughput_std'],
                data['consumer_throughput_avg'], data['consumer_throughput_std'],
                data['iterations']
            ])
    print(f"✅ 结果已保存到: {output_file}")

if __name__ == '__main__':
    base_dir = Path(__file__).parent
    results = analyze_results(base_dir)
    if results:
        print_summary(results)
        save_to_csv(results, base_dir / 'summary.csv')
    else:
        print("未找到测试结果")

ANALYSIS_EOF

chmod +x "$ANALYSIS_SCRIPT"
echo "=============================="
echo "📊 分析脚本已生成（消息大小扩展性测试 - 更新版）"
echo "=============================="
echo "运行以下命令查看结果汇总："
echo "  python3 ${ANALYSIS_SCRIPT}"
echo ""
echo "或直接执行："
echo "  ${ANALYSIS_SCRIPT}"