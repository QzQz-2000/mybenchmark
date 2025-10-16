"""
统计信息聚合工具 - 读取多个consumer的统计文件并合并
"""
import logging
import pickle
from pathlib import Path
from typing import List
from hdrh.histogram import HdrHistogram

logger = logging.getLogger(__name__)


class StatsAggregator:
    """聚合多个consumer进程的统计信息"""

    @staticmethod
    def aggregate_consumer_stats(stats_dir: str, consumer_ids: List[int]) -> dict:
        """
        聚合多个consumer的统计文件。

        :param stats_dir: 统计文件目录
        :param consumer_ids: Consumer ID列表
        :return: 聚合后的统计信息
        """
        stats_path = Path(stats_dir)

        # 初始化聚合结果
        total_messages_received = 0
        merged_end_to_end_latency = None

        successful_reads = 0
        failed_reads = []

        for consumer_id in consumer_ids:
            stats_file = stats_path / f"consumer_{consumer_id}_stats.pkl"

            try:
                if not stats_file.exists():
                    logger.warning(f"Stats file not found: {stats_file}")
                    failed_reads.append(consumer_id)
                    continue

                with open(stats_file, 'rb') as f:
                    data = pickle.load(f)

                # 聚合counter
                total_messages_received += data['messages_received']

                # 反序列化并合并HdrHistogram
                e2e_hist = HdrHistogram.decode(data['end_to_end_latency_encoded'])

                if merged_end_to_end_latency is None:
                    merged_end_to_end_latency = e2e_hist
                else:
                    merged_end_to_end_latency.add(e2e_hist)

                successful_reads += 1
                logger.info(
                    f"Loaded stats from consumer {consumer_id}: "
                    f"{data['messages_received']} messages"
                )

            except Exception as e:
                logger.error(f"Failed to read stats file {stats_file}: {e}", exc_info=True)
                failed_reads.append(consumer_id)

        if successful_reads == 0:
            raise RuntimeError("Failed to read any consumer stats files")

        logger.info(
            f"Successfully aggregated stats from {successful_reads}/{len(consumer_ids)} consumers"
        )

        if failed_reads:
            logger.warning(f"Failed to read stats from consumers: {failed_reads}")

        # 返回聚合结果
        result = {
            'total_messages_received': total_messages_received,
            'end_to_end_latency': merged_end_to_end_latency,
            'successful_consumers': successful_reads,
            'failed_consumers': len(failed_reads),
            'failed_consumer_ids': failed_reads
        }

        return result

    @staticmethod
    def print_aggregated_stats(aggregated: dict):
        """打印聚合后的统计信息"""
        logger.info("=" * 80)
        logger.info("AGGREGATED CONSUMER STATISTICS")
        logger.info("=" * 80)
        logger.info(f"Total messages received: {aggregated['total_messages_received']:,}")
        logger.info(
            f"Successful consumers: {aggregated['successful_consumers']}"
        )

        if aggregated['failed_consumers'] > 0:
            logger.warning(
                f"Failed consumers: {aggregated['failed_consumers']} "
                f"(IDs: {aggregated['failed_consumer_ids']})"
            )

        if aggregated['end_to_end_latency'] and aggregated['end_to_end_latency'].get_total_count() > 0:
            hist = aggregated['end_to_end_latency']
            logger.info(
                f"End-to-end latency (ms) - "
                f"avg: {hist.get_mean_value():.2f}, "
                f"p50: {hist.get_value_at_percentile(50):.2f}, "
                f"p95: {hist.get_value_at_percentile(95):.2f}, "
                f"p99: {hist.get_value_at_percentile(99):.2f}, "
                f"p99.9: {hist.get_value_at_percentile(99.9):.2f}, "
                f"max: {hist.get_max_value():.2f}"
            )
        else:
            logger.warning("No end-to-end latency data available")

        logger.info("=" * 80)

    @staticmethod
    def cleanup_stats_files(stats_dir: str, consumer_ids: List[int]):
        """清理统计文件"""
        stats_path = Path(stats_dir)

        for consumer_id in consumer_ids:
            stats_file = stats_path / f"consumer_{consumer_id}_stats.pkl"
            try:
                if stats_file.exists():
                    stats_file.unlink()
                    logger.debug(f"Deleted stats file: {stats_file}")
            except Exception as e:
                logger.warning(f"Failed to delete stats file {stats_file}: {e}")
