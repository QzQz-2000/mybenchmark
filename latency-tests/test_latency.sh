#!/bin/bash

# ========== 基本参数 ==========
BASE_NAME="Publish Rate Scaling Test"
TOPICS=1
PARTITIONS_PER_TOPIC=1
PRODUCERS_PER_TOPIC=1
SUBSCRIPTIONS_PER_TOPIC=1
CONSUMER_PER_SUBSCRIPTION=1
TEST_DURATION_MINUTES=1
WARMUP_DURATION_MINUTES=0
CONSUMER_BACKLOG_SIZE_GB=0
# 固定的消息大小（1KB）
FIXED_MESSAGE_SIZE=4096 

# ========== 测试参数 ==========
ITERATIONS=1        # 每个生产速率运行1次（根据您最新的输入修改）
RESULTS_BASE_DIR="results_latency_${FIXED_MESSAGE_SIZE}_$(date +%Y%m%d_%H%M%S)"  # 结果根目录

# ========== Kafka Docker 容器名称 ==========
KAFKA_CONTAINER_NAME="kafka"  # 请根据实际容器名称修改

# ========== 起始与最大生产速率（msg/s）==========
RATE=1000           # 起始速率 1000 msg/s
MAX_RATE=4000      # 最大速率 32000 msg/s

# ========== 项目根目录 ==========
# 脚本在 tests 目录中，项目根目录在上一级
DRIVER="kafka"
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKLOADS_DIR="${PROJECT_ROOT}/workloads/rate-scaling-tests"
DRIVER_CONFIG="${PROJECT_ROOT}/examples/${DRIVER}-driver-optimized.yaml"

# ========== 检查 Kafka 配置提示 (由于消息大小固定，这里仅作一般提示) ==========
echo "=============================="
echo " Kafka Configuration Check"
echo "=============================="
echo "⚠️  注意：消息大小固定为 ${FIXED_MESSAGE_SIZE}B"
echo "请确保 Kafka 容器 $KAFKA_CONTAINER_NAME 正在运行"
echo ""
read -p "是否继续运行测试？(y/n): " confirm
if [[ $confirm != [yY] ]]; then
    echo "测试已取消"
    exit 1
fi

# ========== 创建 workloads 目录和结果根目录 ==========
mkdir -p "$WORKLOADS_DIR"
mkdir -p "$RESULTS_BASE_DIR"
echo "✅ 创建结果目录: $RESULTS_BASE_DIR"
echo "✅ 工作负载目录: $WORKLOADS_DIR"
echo ""


# ========== 主测试循环 ==========
# 使用当前速率作为 PRODUCER_RATE
CURRENT_RATE=$RATE
while [ $CURRENT_RATE -le $MAX_RATE ]
do
    # 为当前生产速率创建结果目录
    RATE_DIR="${RESULTS_BASE_DIR}/rate_${CURRENT_RATE}msg_s"
    mkdir -p "$RATE_DIR"

    echo "=============================="
    echo " Testing Publish Rate: ${CURRENT_RATE} msg/s"
    echo "=============================="
    echo "消息大小: ${FIXED_MESSAGE_SIZE}B"
    echo "结果目录: $RATE_DIR"
    echo ""

    # 运行1次迭代
    for iteration in $(seq 1 $ITERATIONS)
    do
        NAME="${BASE_NAME} - ${CURRENT_RATE}msg/s - Run ${iteration}"

        echo "------------------------------"
        echo " Run ${iteration}/${ITERATIONS}"
        echo "------------------------------"

        # 创建本次测试的配置文件（放在 workloads 目录中）
        CONFIG_FILE_NAME="rate_${CURRENT_RATE}msg_s_run${iteration}.yaml"
        CONFIG_FILE="${WORKLOADS_DIR}/${CONFIG_FILE_NAME}"

        cat > $CONFIG_FILE <<EOF
name: ${NAME}

topics: ${TOPICS}
partitionsPerTopic: ${PARTITIONS_PER_TOPIC}
messageSize: ${FIXED_MESSAGE_SIZE}

producersPerTopic: ${PRODUCERS_PER_TOPIC}
producerRate: ${CURRENT_RATE}
subscriptionsPerTopic: ${SUBSCRIPTIONS_PER_TOPIC}
consumerPerSubscription: ${CONSUMER_PER_SUBSCRIPTION}

testDurationMinutes: ${TEST_DURATION_MINUTES}
warmupDurationMinutes: ${WARMUP_DURATION_MINUTES}

consumerBacklogSizeGB: ${CONSUMER_BACKLOG_SIZE_GB}
EOF

        echo "✅ 生成配置文件: ${CONFIG_FILE_NAME}"

        # 准备输出文件
        # JSON 结果保存在每个 rate 的子目录中（相对于项目根目录的路径）
        OUTPUT_FILE_NAME="result_run${iteration}.json"
        OUTPUT_FILE_RELATIVE="$(pwd)/${RATE_DIR}/${OUTPUT_FILE_NAME}"
        MONITOR_LOG="$(pwd)/${RATE_DIR}/monitor_run${iteration}.csv"
        LOG_FILE="$(pwd)/${RATE_DIR}/log_run${iteration}.txt"


        # ========== 运行 OpenMessaging Benchmark ==========
        echo "🚀 开始测试..."
        echo "Driver: $DRIVER_CONFIG"
        echo "Workload: $CONFIG_FILE"
        echo "Output: $OUTPUT_FILE_RELATIVE"
        echo ""

        # 在项目根目录运行 benchmark，指定输出文件位置（相对路径）
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

    echo "✅ 生产速率 ${CURRENT_RATE} msg/s 的所有测试完成"
    echo ""

    # ========== 下一轮（乘 2）==========
    CURRENT_RATE=$((CURRENT_RATE * 2))
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
"""
结果分析脚本（新版）
统计每个生产速率的 End-to-End 延迟（平均值和各分位）、Producer/Consumer 速率、Producer/Consumer 吞吐量
"""

import json
import csv
from pathlib import Path
import statistics

def analyze_results(base_dir):
    results = {}

    for rate_dir in sorted(Path(base_dir).glob("rate_*")):
        rate_name = rate_dir.name
        # 提取速率值
        publish_rate = rate_name.replace("rate_", "").replace("msg_s", "")

        latencies = {'50': [], '75': [], '95': [], '99': []}
        latency_avg = [] # 新增：用于存储 End-to-End 平均延迟
        producer_rates = []
        consumer_rates = []
        producer_throughput = []
        consumer_throughput = []


        # 遍历 JSON 文件
        for result_file in sorted(rate_dir.glob("result_run*.json")):
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
        results[publish_rate] = {}
        # 迭代次数 (用于CSV)
        results[publish_rate]['iterations'] = len(producer_rates) 

        # 统计平均延迟
        results[publish_rate]['latency_avg_avg'] = statistics.mean(latency_avg) if latency_avg else None
        results[publish_rate]['latency_avg_std'] = statistics.stdev(latency_avg) if len(latency_avg) > 1 else 0

        # 统计百分位延迟
        for p in latencies.keys():
            vals = latencies[p]
            results[publish_rate][f'latency_{p}_avg'] = statistics.mean(vals) if vals else None
            results[publish_rate][f'latency_{p}_std'] = statistics.stdev(vals) if len(vals) > 1 else 0

        # 统计速率和吞吐量
        results[publish_rate]['producer_rate_avg'] = statistics.mean(producer_rates) if producer_rates else None
        results[publish_rate]['producer_rate_std'] = statistics.stdev(producer_rates) if len(producer_rates) > 1 else 0
        results[publish_rate]['consumer_rate_avg'] = statistics.mean(consumer_rates) if consumer_rates else None
        results[publish_rate]['consumer_rate_std'] = statistics.stdev(consumer_rates) if len(consumer_rates) > 1 else 0

        results[publish_rate]['producer_throughput_avg'] = statistics.mean(producer_throughput) if producer_throughput else None
        results[publish_rate]['producer_throughput_std'] = statistics.stdev(producer_throughput) if len(producer_throughput) > 1 else 0
        results[publish_rate]['consumer_throughput_avg'] = statistics.mean(consumer_throughput) if consumer_throughput else None
        results[publish_rate]['consumer_throughput_std'] = statistics.stdev(consumer_throughput) if len(consumer_throughput) > 1 else 0

    return results

def print_summary(results):
    print("\n" + "="*140)
    print("测试结果汇总 - 生产速率扩展性测试")
    print("="*140)
    # 头部增加 LatencyAvg(ms)
    header = f"{'目标速率':<12} {'LatencyAvg(ms)':<18} {'Latency50(ms)':<18} {'Latency75(ms)':<18} {'Latency95(ms)':<18} {'Latency99(ms)':<18} {'ProdRate':<12} {'ConsRate':<12} {'ProdThr(MB/s)':<15} {'ConsThr(MB/s)':<15}"
    print(header)
    print("-"*140)

    # 排序和输出行
    for rate in sorted(results.keys(), key=lambda x: int(x)):
        data = results[rate]
        row = f"{rate+'msg/s':<12} "
        
        # 依次输出 Avg, 50, 75, 95, 99 延迟
        # ⚠️ 移除 / 1000 转换
        for p_key in ['avg', '50','75','95','99']:
            avg = data[f'latency_{p_key}_avg']
            std = data[f'latency_{p_key}_std']
            
            # 直接使用原始值 (ms)
            avg_ms = avg
            std_ms = std
            
            row += f"{avg_ms:.2f}±{std_ms:.2f}" if avg_ms is not None else "N/A"
            row += " " * (18 - len(f"{avg_ms:.2f}±{std_ms:.2f}"))
            
        row += f"{data['producer_rate_avg']:.1f}±{data['producer_rate_std']:.1f}".ljust(12)
        row += f"{data['consumer_rate_avg']:.1f}±{data['consumer_rate_std']:.1f}".ljust(12)
        row += f"{data['producer_throughput_avg']:.2f}±{data['producer_throughput_std']:.2f}".ljust(15)
        row += f"{data['consumer_throughput_avg']:.2f}±{data['consumer_throughput_std']:.2f}".ljust(15)
        print(row)
    print("="*140)

def save_to_csv(results, output_file):
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        # 头部增加 LatencyAvg_Avg(ms) 和 LatencyAvg_Std(ms)
        header = ['TargetPublishRate(msg/s)',
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

        for rate in sorted(results.keys(), key=lambda x: int(x)):
            data = results[rate]
            
            # 直接使用原始值 (ms)，不再进行 / 1000 转换
            
            writer.writerow([
                rate,
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
    # 确保脚本路径指向 RESULTS_BASE_DIR
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
echo "📊 分析脚本已生成（速率扩展性测试）"
echo "=============================="
echo "运行以下命令查看结果汇总："
echo "  python3 ${ANALYSIS_SCRIPT}"
echo ""
echo "或直接执行："
echo "  ${ANALYSIS_SCRIPT}"