#!/usr/bin/env python3
"""
消息大小性能实验结果分析脚本
分析不同消息大小对Kafka性能的影响
"""

import json
import glob
import os
from typing import List, Dict
import statistics


def load_results() -> List[Dict]:
    """加载所有msg-size测试结果"""
    results = []

    # 查找所有msg-size开头的JSON文件
    pattern = "msg-size-*-Kafka-*.json"
    files = glob.glob(pattern)

    if not files:
        print("❌ 未找到测试结果文件！")
        print(f"   查找模式: {pattern}")
        print(f"   当前目录: {os.getcwd()}")
        return []

    print(f"📂 找到 {len(files)} 个结果文件\n")

    for file_path in sorted(files):
        with open(file_path, 'r') as f:
            data = json.load(f)
            results.append({
                'file': file_path,
                'data': data
            })

    return results


def calculate_throughput(msg_size_bytes: int, msg_rate: float) -> Dict[str, float]:
    """计算吞吐量 (MB/s)"""
    bytes_per_sec = msg_size_bytes * msg_rate
    mb_per_sec = bytes_per_sec / (1024 * 1024)

    return {
        'msg_rate': msg_rate,
        'bytes_per_sec': bytes_per_sec,
        'mb_per_sec': mb_per_sec
    }


def analyze_results(results: List[Dict]):
    """分析结果并生成对比表"""

    if not results:
        print("没有结果可分析")
        return

    print("=" * 100)
    print("消息大小性能对比分析")
    print("=" * 100)
    print()

    # 表头
    print(f"{'消息大小':<12} {'吞吐量(msg/s)':<15} {'吞吐量(MB/s)':<15} "
          f"{'P50延迟(ms)':<15} {'P99延迟(ms)':<15} {'P99.9延迟(ms)':<15} "
          f"{'错误率':<10} {'积压':<10}")
    print("-" * 100)

    # 数据行
    analysis_data = []

    for result in results:
        data = result['data']
        msg_size = data['messageSize']

        # 计算平均吞吐量
        avg_msg_rate = statistics.mean(data['publishRate'])
        throughput = calculate_throughput(msg_size, avg_msg_rate)

        # 获取延迟指标
        p50 = data['aggregatedPublishLatency50pct']
        p99 = data['aggregatedPublishLatency99pct']
        p999 = data['aggregatedPublishLatency999pct']

        # 获取错误率
        avg_error_rate = statistics.mean(data['publishErrorRate'])

        # 获取平均积压
        avg_backlog = statistics.mean(data['backlog'])

        # 格式化消息大小
        if msg_size >= 1024:
            size_str = f"{msg_size // 1024} KB"
        else:
            size_str = f"{msg_size} B"

        print(f"{size_str:<12} {avg_msg_rate:<15.1f} {throughput['mb_per_sec']:<15.2f} "
              f"{p50:<15.2f} {p99:<15.2f} {p999:<15.2f} "
              f"{avg_error_rate:<10.2f} {avg_backlog:<10.0f}")

        # 保存用于后续分析
        analysis_data.append({
            'msg_size': msg_size,
            'size_str': size_str,
            'msg_rate': avg_msg_rate,
            'mb_per_sec': throughput['mb_per_sec'],
            'p50': p50,
            'p99': p99,
            'p999': p999,
            'error_rate': avg_error_rate,
            'backlog': avg_backlog
        })

    print()
    print("=" * 100)
    print()

    # 关键发现
    print("📊 关键发现:")
    print()

    if len(analysis_data) >= 2:
        # 吞吐量分析
        print("1. 吞吐量趋势:")
        baseline = analysis_data[0]
        for i, item in enumerate(analysis_data):
            if i == 0:
                print(f"   - {item['size_str']}: {item['mb_per_sec']:.2f} MB/s (基线)")
            else:
                ratio = item['mb_per_sec'] / baseline['mb_per_sec']
                change = ((item['mb_per_sec'] - baseline['mb_per_sec']) / baseline['mb_per_sec']) * 100
                print(f"   - {item['size_str']}: {item['mb_per_sec']:.2f} MB/s "
                      f"({change:+.1f}%, {ratio:.2f}x基线)")
        print()

        # 延迟分析
        print("2. P99延迟趋势:")
        for i, item in enumerate(analysis_data):
            if i == 0:
                print(f"   - {item['size_str']}: {item['p99']:.2f} ms (基线)")
            else:
                change = ((item['p99'] - baseline['p99']) / baseline['p99']) * 100
                print(f"   - {item['size_str']}: {item['p99']:.2f} ms ({change:+.1f}%)")
        print()

        # 稳定性分析
        print("3. 稳定性分析:")
        stable_tests = [item for item in analysis_data if item['error_rate'] == 0 and item['backlog'] < 1000]
        if stable_tests:
            print(f"   - {len(stable_tests)}/{len(analysis_data)} 个测试完全稳定 (零错误，低积压)")
            if len(stable_tests) < len(analysis_data):
                unstable = [item for item in analysis_data if item not in stable_tests]
                print(f"   - 不稳定的配置: {', '.join([item['size_str'] for item in unstable])}")
        print()

        # 最优配置推荐
        print("4. 推荐配置:")
        best_throughput = max(analysis_data, key=lambda x: x['mb_per_sec'])
        best_latency = min(analysis_data, key=lambda x: x['p99'])

        print(f"   - 最高吞吐量: {best_throughput['size_str']} ({best_throughput['mb_per_sec']:.2f} MB/s)")
        print(f"   - 最低延迟:   {best_latency['size_str']} (P99={best_latency['p99']:.2f} ms)")

        # 综合评分 (吞吐量 / 延迟)
        for item in analysis_data:
            item['score'] = item['mb_per_sec'] / (item['p99'] / 100)  # 归一化延迟

        best_overall = max(analysis_data, key=lambda x: x['score'])
        print(f"   - 综合最优:   {best_overall['size_str']} (高吞吐+低延迟)")
        print()


def main():
    print()
    print("🔬 消息大小性能实验结果分析")
    print()

    results = load_results()

    if results:
        analyze_results(results)
        print("✅ 分析完成")
    else:
        print("❌ 没有找到实验结果")
        print()
        print("请先运行实验:")
        print("  bash run_message_size_experiment.sh")

    print()


if __name__ == '__main__':
    main()
