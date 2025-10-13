#!/usr/bin/env python3
"""
控制变量实验结果分析
固定吞吐量，对比不同消息大小对延迟的影响
"""

import json
import glob
import statistics


def load_results():
    """加载所有controlled实验结果"""
    results = []
    pattern = "msg-size-*-controlled-Kafka-*.json"
    files = glob.glob(pattern)

    if not files:
        print("❌ 未找到控制变量实验结果文件！")
        print(f"   查找模式: {pattern}")
        return []

    print(f"📂 找到 {len(files)} 个结果文件\n")

    for file_path in sorted(files):
        with open(file_path, 'r') as f:
            data = json.load(f)
            results.append({'file': file_path, 'data': data})

    return results


def analyze_controlled_experiment(results):
    """分析控制变量实验"""

    if not results:
        return

    print("=" * 120)
    print("控制变量实验分析：固定吞吐量，对比延迟")
    print("=" * 120)
    print()

    # 表头
    print(f"{'消息大小':<12} {'msg/s':<12} {'吞吐(MB/s)':<12} "
          f"{'P50(ms)':<12} {'P99(ms)':<12} {'P99.9(ms)':<12} "
          f"{'错误率':<10} {'积压':<10}")
    print("-" * 120)

    analysis_data = []

    for result in results:
        data = result['data']
        msg_size = data['messageSize']

        # 计算指标
        avg_msg_rate = statistics.mean(data['publishRate'])
        throughput_mbps = (avg_msg_rate * msg_size) / (1024 * 1024)

        p50 = data['aggregatedPublishLatency50pct']
        p99 = data['aggregatedPublishLatency99pct']
        p999 = data['aggregatedPublishLatency999pct']

        avg_error_rate = statistics.mean(data['publishErrorRate'])
        avg_backlog = statistics.mean(data['backlog'])

        # 格式化大小
        if msg_size >= 1024:
            size_str = f"{msg_size // 1024} KB"
        else:
            size_str = f"{msg_size} B"

        print(f"{size_str:<12} {avg_msg_rate:<12.1f} {throughput_mbps:<12.2f} "
              f"{p50:<12.2f} {p99:<12.2f} {p999:<12.2f} "
              f"{avg_error_rate:<10.2f} {avg_backlog:<10.0f}")

        analysis_data.append({
            'msg_size': msg_size,
            'size_str': size_str,
            'msg_rate': avg_msg_rate,
            'mb_per_sec': throughput_mbps,
            'p50': p50,
            'p99': p99,
            'p999': p999,
            'error_rate': avg_error_rate,
            'backlog': avg_backlog
        })

    print()
    print("=" * 120)
    print()

    # 关键发现
    print("📊 实验结果分析:")
    print()

    if len(analysis_data) >= 2:
        # 1. 吞吐量一致性验证
        print("1. 吞吐量控制验证:")
        throughputs = [item['mb_per_sec'] for item in analysis_data]
        avg_throughput = statistics.mean(throughputs)
        std_throughput = statistics.stdev(throughputs) if len(throughputs) > 1 else 0

        print(f"   平均吞吐量: {avg_throughput:.2f} MB/s")
        print(f"   标准差: {std_throughput:.2f} MB/s")
        if std_throughput / avg_throughput < 0.2:
            print(f"   ✅ 变异系数 < 20%，吞吐量控制良好")
        else:
            print(f"   ⚠️  变异系数 > 20%，吞吐量控制欠佳")
        print()

        # 2. 延迟随消息大小的变化
        print("2. 延迟 vs 消息大小:")
        baseline = analysis_data[0]
        print(f"   基线 ({baseline['size_str']}): P99={baseline['p99']:.2f} ms")

        for i, item in enumerate(analysis_data[1:], 1):
            ratio = item['msg_size'] / baseline['msg_size']
            latency_increase = ((item['p99'] - baseline['p99']) / baseline['p99']) * 100

            print(f"   {item['size_str']}: P99={item['p99']:.2f} ms "
                  f"(消息大小 {ratio:.1f}x, 延迟增加 {latency_increase:+.1f}%)")
        print()

        # 3. 延迟增长率分析
        print("3. 延迟增长特征:")
        sizes = [item['msg_size'] for item in analysis_data]
        p99s = [item['p99'] for item in analysis_data]

        # 计算延迟增长是否线性
        if len(sizes) >= 3:
            # 简单的线性判断：看中间点
            mid_idx = len(sizes) // 2
            size_ratio = sizes[mid_idx] / sizes[0]
            latency_ratio = p99s[mid_idx] / p99s[0]

            if latency_ratio < size_ratio * 0.8:
                print(f"   📈 延迟增长 < 消息大小增长 → 大消息有批处理优势")
            elif latency_ratio > size_ratio * 1.5:
                print(f"   📈 延迟增长 > 消息大小增长 → 大消息有额外开销")
            else:
                print(f"   📈 延迟增长 ≈ 消息大小增长 → 近似线性关系")
        print()

        # 4. 稳定性分析
        print("4. 系统稳定性:")
        stable_tests = [item for item in analysis_data if item['error_rate'] == 0 and item['backlog'] < 1000]
        print(f"   稳定测试: {len(stable_tests)}/{len(analysis_data)}")

        if len(stable_tests) == len(analysis_data):
            print(f"   ✅ 所有测试稳定运行")
        else:
            unstable = [item for item in analysis_data if item not in stable_tests]
            print(f"   ⚠️  不稳定配置: {', '.join([item['size_str'] for item in unstable])}")
        print()

        # 5. 推荐配置
        print("5. 推荐配置:")
        best_latency = min(analysis_data, key=lambda x: x['p99'])
        best_throughput = max(analysis_data, key=lambda x: x['mb_per_sec'])

        print(f"   最低延迟: {best_latency['size_str']} (P99={best_latency['p99']:.2f} ms)")
        print(f"   最高吞吐: {best_throughput['size_str']} ({best_throughput['mb_per_sec']:.2f} MB/s)")

        # 找到性价比最高的配置（延迟适中，吞吐量高）
        for item in analysis_data:
            item['score'] = item['mb_per_sec'] / (item['p99'] / 1000)  # MB/s per second latency

        best_value = max(analysis_data, key=lambda x: x['score'])
        print(f"   性价比最优: {best_value['size_str']} (平衡吞吐与延迟)")
        print()


def main():
    print()
    print("🔬 控制变量实验结果分析")
    print()

    results = load_results()

    if results:
        analyze_controlled_experiment(results)
        print("✅ 分析完成")
    else:
        print("❌ 没有找到实验结果")
        print()
        print("请先运行控制变量实验:")
        print("  bash run_controlled_experiment.sh")

    print()


if __name__ == '__main__':
    main()
