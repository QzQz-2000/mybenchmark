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

