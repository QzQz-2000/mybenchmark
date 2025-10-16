#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
比较Queue统计和File统计的差异
用于验证双输出模式的正确性和性能对比
"""

import json
import logging
import sys
from pathlib import Path
from typing import Dict, Optional

from benchmark.utils.file_stats_reader import FileStatsReader


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class StatsComparator:
    """比较Queue-based和File-based统计结果"""

    def __init__(self, json_result_path: Optional[str] = None):
        self.json_path = json_result_path
        self.file_reader = FileStatsReader()

    def load_queue_stats(self) -> Optional[Dict]:
        """从JSON结果文件加载Queue统计数据"""
        if not self.json_path:
            logger.warning("No JSON result path provided")
            return None

        json_file = Path(self.json_path)
        if not json_file.exists():
            logger.error(f"JSON result file not found: {json_file}")
            return None

        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
            logger.info(f"Loaded Queue stats from {json_file}")
            return data
        except Exception as e:
            logger.error(f"Failed to load JSON result: {e}")
            return None

    def load_file_stats(self) -> Dict:
        """加载File统计数据"""
        return self.file_reader.read_consumer_stats()

    def compare(self):
        """比较两种统计方法的结果"""
        print("\n" + "="*100)
        print("STATISTICS METHOD COMPARISON: Queue-based vs File-based")
        print("="*100 + "\n")

        # 加载Queue统计
        queue_stats = self.load_queue_stats()

        # 加载File统计
        file_stats = self.load_file_stats()

        # 显示Queue统计
        if queue_stats:
            self._print_queue_stats(queue_stats)
        else:
            print("Queue-based stats: NOT AVAILABLE\n")

        # 显示File统计
        self._print_file_stats(file_stats)

        # 对比分析
        if queue_stats and not file_stats.get('error'):
            self._print_comparison(queue_stats, file_stats)

    def _print_queue_stats(self, stats: Dict):
        """打印Queue统计摘要"""
        print("-" * 100)
        print("QUEUE-BASED STATISTICS (from JSON result)")
        print("-" * 100)

        print(f"Workload:            {stats.get('workload', 'N/A')}")
        print(f"Driver:              {stats.get('driver', 'N/A')}")
        print(f"Message Size:        {stats.get('messageSize', 0):,} bytes")
        print(f"Topics:              {stats.get('topics', 0)}")
        print(f"Partitions:          {stats.get('partitions', 0)}")
        print(f"Producers:           {stats.get('producersPerTopic', 0)}")
        print(f"Consumers:           {stats.get('consumersPerTopic', 0)}")

        # 计算总消息数（从consumeRate推算）
        consume_rates = stats.get('consumeRate', [])
        if consume_rates:
            avg_consume_rate = sum(consume_rates) / len(consume_rates)
            print(f"\nAverage Consume Rate: {avg_consume_rate:.2f} msg/s")

        # E2E延迟
        print(f"\nEnd-to-End Latency (Queue):")
        print(f"  Average:           {stats.get('aggregatedEndToEndLatencyAvg', 0):.2f} ms")
        print(f"  Maximum:           {stats.get('aggregatedEndToEndLatencyMax', 0)} ms")

        e2e_quantiles = stats.get('aggregatedEndToEndLatencyQuantiles', {})
        if e2e_quantiles:
            print(f"\nPercentiles:")
            print(f"  p50:               {e2e_quantiles.get('50', 0)} ms")
            print(f"  p75:               {e2e_quantiles.get('75', 0)} ms")
            print(f"  p90:               {e2e_quantiles.get('90', 0)} ms")
            print(f"  p95:               {e2e_quantiles.get('95', 0)} ms")
            print(f"  p99:               {e2e_quantiles.get('99', 0)} ms")
            print(f"  p99.9:             {e2e_quantiles.get('99.9', 0)} ms")
            print(f"  p99.99:            {e2e_quantiles.get('99.99', 0)} ms")

        print()

    def _print_file_stats(self, stats: Dict):
        """打印File统计摘要"""
        print("-" * 100)
        print("FILE-BASED STATISTICS (from /tmp/kafka_benchmark_stats/)")
        print("-" * 100)

        if stats.get('error'):
            print(f"ERROR: {stats['error']}\n")
            return

        print(f"Total Consumers:     {stats['consumer_count']}")
        print(f"Total Messages:      {stats['total_messages']:,}")
        print(f"Histogram Samples:   {stats.get('histogram_total_count', 0):,}")

        if stats.get('failed_reads', 0) > 0:
            print(f"Failed Reads:        {stats['failed_reads']}")

        print(f"\nEnd-to-End Latency (File):")
        print(f"  Average:           {stats['e2e_latency_avg']:.2f} ms")
        print(f"  Maximum:           {stats['e2e_latency_max']} ms")

        percentiles = stats.get('e2e_latency_percentiles', {})
        if percentiles:
            print(f"\nPercentiles:")
            print(f"  p50:               {percentiles.get('50', 0)} ms")
            print(f"  p75:               {percentiles.get('75', 0)} ms")
            print(f"  p90:               {percentiles.get('90', 0)} ms")
            print(f"  p95:               {percentiles.get('95', 0)} ms")
            print(f"  p99:               {percentiles.get('99', 0)} ms")
            print(f"  p99.9:             {percentiles.get('99.9', 0)} ms")
            print(f"  p99.99:            {percentiles.get('99.99', 0)} ms")

        print()

    def _print_comparison(self, queue_stats: Dict, file_stats: Dict):
        """打印对比分析"""
        print("-" * 100)
        print("COMPARISON ANALYSIS")
        print("-" * 100)

        # 对比平均延迟
        queue_avg = queue_stats.get('aggregatedEndToEndLatencyAvg', 0)
        file_avg = file_stats.get('e2e_latency_avg', 0)
        diff_avg = file_avg - queue_avg
        diff_pct = (diff_avg / queue_avg * 100) if queue_avg > 0 else 0

        print(f"\nAverage E2E Latency Comparison:")
        print(f"  Queue:             {queue_avg:.2f} ms")
        print(f"  File:              {file_avg:.2f} ms")
        print(f"  Difference:        {diff_avg:+.2f} ms ({diff_pct:+.2f}%)")

        # 对比百分位数
        queue_percentiles = queue_stats.get('aggregatedEndToEndLatencyQuantiles', {})
        file_percentiles = file_stats.get('e2e_latency_percentiles', {})

        print(f"\nPercentile Comparison:")
        print(f"  {'Percentile':<12} {'Queue (ms)':<15} {'File (ms)':<15} {'Diff (ms)':<15} {'Diff %':<10}")
        print(f"  {'-'*12} {'-'*15} {'-'*15} {'-'*15} {'-'*10}")

        for p in ['50', '75', '90', '95', '99', '99.9', '99.99']:
            queue_val = queue_percentiles.get(p, 0)
            file_val = file_percentiles.get(p, 0)
            diff = file_val - queue_val
            diff_pct = (diff / queue_val * 100) if queue_val > 0 else 0

            print(f"  p{p:<11} {queue_val:<15} {file_val:<15} {diff:+<15.0f} {diff_pct:+.2f}%")

        # 对比最大延迟
        queue_max = queue_stats.get('aggregatedEndToEndLatencyMax', 0)
        file_max = file_stats.get('e2e_latency_max', 0)
        diff_max = file_max - queue_max
        diff_max_pct = (diff_max / queue_max * 100) if queue_max > 0 else 0

        print(f"\nMaximum E2E Latency Comparison:")
        print(f"  Queue:             {queue_max} ms")
        print(f"  File:              {file_max} ms")
        print(f"  Difference:        {diff_max:+} ms ({diff_max_pct:+.2f}%)")

        # 结论
        print(f"\nConclusion:")
        if abs(diff_pct) < 5:
            print(f"  ✓ Both methods show SIMILAR results (within 5% difference)")
            print(f"  ✓ File-based statistics are ACCURATE")
        elif diff_pct > 0:
            print(f"  ⚠ File-based shows HIGHER latency by {diff_pct:.2f}%")
            print(f"  → May indicate file I/O overhead or timing differences")
        else:
            print(f"  ⚠ Queue-based shows HIGHER latency by {abs(diff_pct):.2f}%")
            print(f"  → May indicate Queue serialization overhead")

        print("="*100 + "\n")


def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("Usage: python -m benchmark.utils.compare_stats_methods <json_result_file>")
        print("\nExample:")
        print("  python -m benchmark.utils.compare_stats_methods 1-topic-1-partition-1kb-Kafka-2025-10-16-01-48-39.json")
        sys.exit(1)

    json_path = sys.argv[1]
    comparator = StatsComparator(json_path)
    comparator.compare()


if __name__ == "__main__":
    main()
