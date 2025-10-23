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

