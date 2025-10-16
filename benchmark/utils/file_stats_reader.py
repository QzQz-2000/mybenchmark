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
文件统计读取和聚合工具
用于从 /tmp/kafka_benchmark_stats/ 读取consumer统计文件并聚合
"""

import pickle
import logging
from pathlib import Path
from typing import List, Dict, Optional
from hdrh.histogram import HdrHistogram


logger = logging.getLogger(__name__)


class FileStatsReader:
    """读取和聚合基于文件的统计数据"""

    def __init__(self, stats_dir: str = None):
        # ✅ 默认使用当前工作目录下的 benchmark_results 文件夹
        if stats_dir is None:
            stats_dir = Path.cwd() / "benchmark_results"
        self.stats_dir = Path(stats_dir)

    def read_consumer_stats(self, consumer_ids: Optional[List[int]] = None) -> Dict:
        """
        读取并聚合consumer统计数据

        :param consumer_ids: 要读取的consumer ID列表，None表示读取所有
        :return: 聚合后的统计字典
        """
        if not self.stats_dir.exists():
            logger.warning(f"Stats directory does not exist: {self.stats_dir}")
            return {
                'total_messages': 0,
                'consumer_count': 0,
                'e2e_latency_percentiles': {},
                'error': 'Stats directory not found'
            }

        # 查找所有consumer统计文件
        consumer_files = list(self.stats_dir.glob("consumer_*_stats.pkl"))

        if not consumer_files:
            logger.warning(f"No consumer stats files found in {self.stats_dir}")
            return {
                'total_messages': 0,
                'consumer_count': 0,
                'e2e_latency_percentiles': {},
                'error': 'No stats files found'
            }

        logger.info(f"Found {len(consumer_files)} consumer stats files")

        # 聚合统计
        total_messages = 0
        merged_histogram = None
        consumer_count = 0
        failed_reads = 0

        for stats_file in consumer_files:
            try:
                # 读取pickle文件
                with open(stats_file, 'rb') as f:
                    file_data = pickle.load(f)

                agent_id = file_data.get('agent_id', '?')

                # 如果指定了consumer_ids，只处理指定的consumer
                if consumer_ids is not None and agent_id not in consumer_ids:
                    continue

                # 累加消息数
                total_messages += file_data.get('total_messages_received', 0)
                consumer_count += 1

                # 解码并合并histogram
                histogram_encoded = file_data.get('e2e_latency_histogram_encoded')
                if histogram_encoded:
                    try:
                        # 解码histogram
                        consumer_histogram = HdrHistogram.decode(histogram_encoded)

                        # 合并到总histogram
                        if merged_histogram is None:
                            merged_histogram = consumer_histogram
                        else:
                            merged_histogram.add(consumer_histogram)

                        logger.info(f"Merged histogram from consumer {agent_id}: "
                                  f"{file_data.get('total_messages_received', 0)} messages")
                    except Exception as e:
                        logger.error(f"Failed to decode histogram from {stats_file}: {e}")

            except Exception as e:
                logger.error(f"Failed to read stats file {stats_file}: {e}")
                failed_reads += 1

        if consumer_count == 0:
            logger.warning("No valid consumer stats data found")
            return {
                'total_messages': 0,
                'consumer_count': 0,
                'e2e_latency_percentiles': {},
                'error': 'No valid stats data'
            }

        # 计算延迟百分位数
        percentiles = {}
        avg_latency = 0.0
        max_latency = 0

        if merged_histogram and merged_histogram.get_total_count() > 0:
            percentiles = {
                '50': merged_histogram.get_value_at_percentile(50.0),
                '75': merged_histogram.get_value_at_percentile(75.0),
                '90': merged_histogram.get_value_at_percentile(90.0),
                '95': merged_histogram.get_value_at_percentile(95.0),
                '99': merged_histogram.get_value_at_percentile(99.0),
                '99.9': merged_histogram.get_value_at_percentile(99.9),
                '99.99': merged_histogram.get_value_at_percentile(99.99),
            }
            avg_latency = merged_histogram.get_mean_value()
            max_latency = merged_histogram.get_max_value()

        result = {
            'total_messages': total_messages,
            'consumer_count': consumer_count,
            'failed_reads': failed_reads,
            'e2e_latency_avg': avg_latency,
            'e2e_latency_max': max_latency,
            'e2e_latency_percentiles': percentiles,
            'histogram_total_count': merged_histogram.get_total_count() if merged_histogram else 0
        }

        logger.info(f"Aggregated stats from {consumer_count} consumers: "
                   f"{total_messages} messages, "
                   f"avg latency={avg_latency:.2f}ms, "
                   f"p50={percentiles.get('50', 0)}ms, "
                   f"p99={percentiles.get('99', 0)}ms")

        return result

    def cleanup_stats_files(self):
        """清理所有统计文件"""
        if not self.stats_dir.exists():
            return

        deleted = 0
        for stats_file in self.stats_dir.glob("*.pkl"):
            try:
                stats_file.unlink()
                deleted += 1
            except Exception as e:
                logger.error(f"Failed to delete {stats_file}: {e}")

        # 尝试删除临时文件
        for temp_file in self.stats_dir.glob("*.tmp"):
            try:
                temp_file.unlink()
                deleted += 1
            except Exception as e:
                logger.error(f"Failed to delete {temp_file}: {e}")

        logger.info(f"Cleaned up {deleted} stats files from {self.stats_dir}")

    def print_stats_summary(self, stats: Dict):
        """打印统计摘要"""
        print("\n" + "="*80)
        print("FILE-BASED STATISTICS SUMMARY")
        print("="*80)

        if 'error' in stats:
            print(f"ERROR: {stats['error']}")
            return

        print(f"Total Consumers:     {stats['consumer_count']}")
        print(f"Total Messages:      {stats['total_messages']:,}")
        print(f"Histogram Samples:   {stats.get('histogram_total_count', 0):,}")

        if stats.get('failed_reads', 0) > 0:
            print(f"Failed Reads:        {stats['failed_reads']}")

        print(f"\nEnd-to-End Latency:")
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

        print("="*80 + "\n")


if __name__ == "__main__":
    """测试用法：python -m benchmark.utils.file_stats_reader"""
    logging.basicConfig(level=logging.INFO)

    reader = FileStatsReader()
    stats = reader.read_consumer_stats()
    reader.print_stats_summary(stats)
