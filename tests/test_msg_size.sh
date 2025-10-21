#!/bin/bash

# ========== 基本参数 ==========
BASE_NAME="Message Size Test"
TOPICS=1
PARTITIONS_PER_TOPIC=16
PRODUCERS_PER_TOPIC=4
PRODUCER_RATE=30000
SUBSCRIPTIONS_PER_TOPIC=1
CONSUMER_PER_SUBSCRIPTION=4
TEST_DURATION_MINUTES=1
WARMUP_DURATION_MINUTES=0
CONSUMER_BACKLOG_SIZE_GB=0

# ========== 测试参数 ==========
ITERATIONS=1        # 每个消息大小运行10次
RESULTS_BASE_DIR="results_$(date +%Y%m%d_%H%M%S)"  # 结果根目录

# ========== Kafka Docker 容器名称 ==========
KAFKA_CONTAINER_NAME="kafka"  # 请根据实际容器名称修改

# ========== 起始与最大大小（字节）==========
SIZE=8              # 起始大小 8B
MAX_SIZE=$((128 * 1024))
# MAX_SIZE=$((128 * 1024 * 1024))  # 128MB

# ========== 项目根目录 ==========
# 脚本在 tests 目录中，项目根目录在上一级
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKLOADS_DIR="${PROJECT_ROOT}/workloads/msg-size-tests"
DRIVER_CONFIG="${PROJECT_ROOT}/examples/kafka-driver-max-throughput.yaml"

# ========== 检查 Kafka 配置提示 ==========
echo "=============================="
echo " Kafka Configuration Check"
echo "=============================="
echo "⚠️  重要提示：测试最大消息大小为 128MB"
echo "请确保 Kafka 配置支持大消息传输，需要设置："
echo "  - message.max.bytes >= 134217728 (128MB)"
echo "  - replica.fetch.max.bytes >= 134217728"
echo "  - max.request.size >= 134217728 (producer)"
echo "  - fetch.max.bytes >= 134217728 (consumer)"
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

# ========== 检查 Kafka 容器 ==========
echo "=============================="
echo " Checking Kafka Container"
echo "=============================="
if ! docker ps --format '{{.Names}}' | grep -q "^${KAFKA_CONTAINER_NAME}$"; then
    echo "⚠️  警告：未找到名为 '${KAFKA_CONTAINER_NAME}' 的运行中 Kafka 容器"
    echo "可用的容器："
    docker ps --format 'table {{.Names}}\t{{.Image}}'
    echo ""
    read -p "请输入正确的 Kafka 容器名称（留空跳过监控）: " input_name
    if [ -n "$input_name" ]; then
        KAFKA_CONTAINER_NAME="$input_name"
    else
        echo "⚠️  将跳过 Kafka 资源监控"
        KAFKA_CONTAINER_NAME=""
    fi
else
    echo "✅ 找到 Kafka 容器: ${KAFKA_CONTAINER_NAME}"
fi
echo ""

# ========== 资源监控函数 ==========
# 启动资源监控
start_monitoring() {
    local log_file=$1
    local container_name=$2

    if [ -z "$container_name" ]; then
        return
    fi

    # 直接在后台启动监控，使用 nohup 确保完全独立
    nohup bash -c "
        echo 'Timestamp,CPU%,Memory%' > '$log_file'
        while true; do
            stats=\$(docker stats '$container_name' --no-stream --format '{{.CPUPerc}},{{.MemPerc}}' 2>/dev/null)
            if [ \$? -eq 0 ]; then
                timestamp=\$(date +%s)
                echo \"\$timestamp,\$stats\" >> '$log_file'
            fi
            sleep 5
        done
    " > /dev/null 2>&1 &

    local pid=$!

    echo $pid  # 返回监控进程的PID
}

# 停止资源监控
stop_monitoring() {
    local monitor_pid=$1
    if [ -n "$monitor_pid" ] && kill -0 "$monitor_pid" 2>/dev/null; then
        kill "$monitor_pid" 2>/dev/null
        wait "$monitor_pid" 2>/dev/null
    fi
}

# ========== 主测试循环 ==========
while [ $SIZE -le $MAX_SIZE ]
do
    # 为当前消息大小创建结果目录
    SIZE_DIR="${RESULTS_BASE_DIR}/size_${SIZE}B"
    mkdir -p "$SIZE_DIR"

    echo "=============================="
    echo " Testing Message Size: ${SIZE}B"
    echo "=============================="
    echo "结果目录: $SIZE_DIR"
    echo ""

    # 运行10次迭代
    for iteration in $(seq 1 $ITERATIONS)
    do
        NAME="${BASE_NAME} - ${SIZE}B - Run ${iteration}"

        echo "------------------------------"
        echo " Run ${iteration}/${ITERATIONS}"
        echo "------------------------------"

        # 创建本次测试的配置文件（放在 workloads 目录中）
        CONFIG_FILE_NAME="msg_size_${SIZE}B_run${iteration}.yaml"
        CONFIG_FILE="${WORKLOADS_DIR}/${CONFIG_FILE_NAME}"

        cat > $CONFIG_FILE <<EOF
name: ${NAME}

topics: ${TOPICS}
partitionsPerTopic: ${PARTITIONS_PER_TOPIC}
messageSize: ${SIZE}

producersPerTopic: ${PRODUCERS_PER_TOPIC}
producerRate: ${PRODUCER_RATE}
subscriptionsPerTopic: ${SUBSCRIPTIONS_PER_TOPIC}
consumerPerSubscription: ${CONSUMER_PER_SUBSCRIPTION}

testDurationMinutes: ${TEST_DURATION_MINUTES}
warmupDurationMinutes: ${WARMUP_DURATION_MINUTES}

consumerBacklogSizeGB: ${CONSUMER_BACKLOG_SIZE_GB}
EOF

        echo "✅ 生成配置文件: ${CONFIG_FILE_NAME}"

        # 准备输出文件
        # JSON 结果保存在每个 size 的子目录中（相对于项目根目录的路径）
        OUTPUT_FILE_NAME="result_run${iteration}.json"
        OUTPUT_FILE_RELATIVE="tests/${SIZE_DIR}/${OUTPUT_FILE_NAME}"
        MONITOR_LOG="$(pwd)/${SIZE_DIR}/monitor_run${iteration}.csv"
        LOG_FILE="$(pwd)/${SIZE_DIR}/log_run${iteration}.txt"

        # 启动资源监控
        MONITOR_PID=""
        if [ -n "$KAFKA_CONTAINER_NAME" ]; then
            MONITOR_PID=$(start_monitoring "$MONITOR_LOG" "$KAFKA_CONTAINER_NAME")
            echo "✅ 启动资源监控 (PID: $MONITOR_PID)"
        fi

        # ========== 运行 OpenMessaging Benchmark ==========
        echo "🚀 开始测试..."
        echo "Driver: $DRIVER_CONFIG"
        echo "Workload: $CONFIG_FILE"
        echo "Output: $OUTPUT_FILE_RELATIVE"
        echo ""

        # 在项目根目录运行 benchmark，指定输出文件位置（相对路径）
        (cd "$PROJECT_ROOT" && python -m benchmark -d "$DRIVER_CONFIG" -o "$OUTPUT_FILE_RELATIVE" "$CONFIG_FILE") 2>&1 | tee "$LOG_FILE"
        BENCHMARK_EXIT_CODE=$?

        # 停止资源监控
        if [ -n "$MONITOR_PID" ]; then
            stop_monitoring "$MONITOR_PID"
            echo "✅ 停止资源监控"
        fi

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
    SIZE=$((SIZE * 4))
done

echo "=============================="
echo "✅ 所有测试完成！"
echo "=============================="
echo "结果保存在: $RESULTS_BASE_DIR"
echo ""

# ========== 生成汇总分析脚本（新版） ==========
ANALYSIS_SCRIPT="${RESULTS_BASE_DIR}/analyze_results.py"
cat > "$ANALYSIS_SCRIPT" <<'ANALYSIS_EOF'
#!/usr/bin/env python3
#!/usr/bin/env python3
"""
结果分析脚本（新版）
统计每个消息大小的 End-to-End 延迟各分位、Producer/Consumer 速率、Producer/Consumer 吞吐量，以及 CPU/Memory 平均值
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
        producer_rates = []
        consumer_rates = []
        producer_throughput = []
        consumer_throughput = []
        cpu_usages = []
        mem_usages = []

        # 遍历 JSON 文件
        for result_file in sorted(size_dir.glob("result_run*.json")):
            with open(result_file, 'r') as f:
                data = json.load(f)
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

        # 遍历 CSV 文件获取 CPU/Memory
        for monitor_file in sorted(size_dir.glob("monitor_run*.csv")):
            with open(monitor_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cpu_usages.append(float(row['CPU%'].replace('%','')))
                    mem_usages.append(float(row['Memory%'].replace('%','')))

        # 统计平均值和标准差
        results[message_size] = {}
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

        # CPU 和内存只取平均值
        results[message_size]['cpu_avg'] = statistics.mean(cpu_usages) if cpu_usages else None
        results[message_size]['mem_avg'] = statistics.mean(mem_usages) if mem_usages else None
        results[message_size]['iterations'] = len(producer_rates)

    return results

def print_summary(results):
    print("\n" + "="*140)
    print("测试结果汇总")
    print("="*140)
    header = f"{'消息大小':<10} {'Latency50(ms)':<18} {'Latency75(ms)':<18} {'Latency95(ms)':<18} {'Latency99(ms)':<18} {'ProdRate':<12} {'ConsRate':<12} {'ProdThr(MB/s)':<15} {'ConsThr(MB/s)':<15} {'CPU%':<10} {'Memory%':<10}"
    print(header)
    print("-"*140)

    for size in sorted(results.keys(), key=lambda x: int(x)):
        data = results[size]
        row = f"{size+'B':<10} "
        for p in ['50','75','95','99']:
            avg = data[f'latency_{p}_avg']
            std = data[f'latency_{p}_std']
            row += f"{avg:.2f}±{std:.2f}" if avg is not None else "N/A"
            row += " " * (18 - len(f"{avg:.2f}±{std:.2f}"))
        row += f"{data['producer_rate_avg']:.1f}±{data['producer_rate_std']:.1f}".ljust(12)
        row += f"{data['consumer_rate_avg']:.1f}±{data['consumer_rate_std']:.1f}".ljust(12)
        row += f"{data['producer_throughput_avg']:.2f}±{data['producer_throughput_std']:.2f}".ljust(15)
        row += f"{data['consumer_throughput_avg']:.2f}±{data['consumer_throughput_std']:.2f}".ljust(15)
        row += f"{data['cpu_avg']:.1f}".ljust(10)
        row += f"{data['mem_avg']:.1f}".ljust(10)
        print(row)
    print("="*140)

def save_to_csv(results, output_file):
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        header = ['MessageSize(B)',
                  'Latency50_Avg(ms)','Latency50_Std(ms)',
                  'Latency75_Avg(ms)','Latency75_Std(ms)',
                  'Latency95_Avg(ms)','Latency95_Std(ms)',
                  'Latency99_Avg(ms)','Latency99_Std(ms)',
                  'ProducerRate_Avg','ProducerRate_Std',
                  'ConsumerRate_Avg','ConsumerRate_Std',
                  'ProducerThroughput_Avg(MB/s)','ProducerThroughput_Std(MB/s)',
                  'ConsumerThroughput_Avg(MB/s)','ConsumerThroughput_Std(MB/s)',
                  'CPU_Avg','Memory_Avg','Iterations']
        writer.writerow(header)

        for size in sorted(results.keys(), key=lambda x: int(x)):
            data = results[size]
            writer.writerow([
                size,
                data['latency_50_avg'], data['latency_50_std'],
                data['latency_75_avg'], data['latency_75_std'],
                data['latency_95_avg'], data['latency_95_std'],
                data['latency_99_avg'], data['latency_99_std'],
                data['producer_rate_avg'], data['producer_rate_std'],
                data['consumer_rate_avg'], data['consumer_rate_std'],
                data['producer_throughput_avg'], data['producer_throughput_std'],
                data['consumer_throughput_avg'], data['consumer_throughput_std'],
                data['cpu_avg'], data['mem_avg'],
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
echo "📊 分析脚本已生成（新版 End-to-End 延迟 & Producer/Consumer rate）"
echo "=============================="
echo "运行以下命令查看结果汇总："
echo "  python3 ${ANALYSIS_SCRIPT}"
echo ""
echo "或直接执行："
echo "  ${ANALYSIS_SCRIPT}"
echo ""