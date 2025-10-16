#!/usr/bin/env python3
"""
测试隔离consumer架构的示例脚本
"""
import logging
import time
from benchmark.drivers.kafka.kafka_benchmark_consumer_isolated import KafkaBenchmarkConsumerIsolated
from benchmark.utils.stats_aggregator import StatsAggregator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_isolated_consumers():
    """测试多个隔离的consumer"""

    # Kafka配置
    kafka_properties = {
        'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'session.timeout.ms': 30000,
        'max.poll.interval.ms': 300000,
    }

    topic = "test-topic-0"
    subscription_name = "test-consumer-group"
    num_consumers = 10
    stats_dir = "/tmp/kafka_benchmark_stats_test"

    logger.info("=" * 80)
    logger.info(f"Creating {num_consumers} isolated consumers")
    logger.info("=" * 80)

    # 创建多个consumer
    consumers = []
    consumer_ids = []

    for i in range(num_consumers):
        consumer = KafkaBenchmarkConsumerIsolated(
            topic=topic,
            subscription_name=subscription_name,
            properties=kafka_properties.copy(),
            stats_dir=stats_dir,
            poll_timeout=1.0,
            commit_interval=1000
        )
        consumers.append(consumer)
        consumer_ids.append(consumer.consumer_id)

    logger.info(f"Created {len(consumers)} consumers with IDs: {consumer_ids}")

    # 运行一段时间
    test_duration = 30  # 30秒
    logger.info(f"Running test for {test_duration} seconds...")

    try:
        time.sleep(test_duration)
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")

    # 停止所有consumer
    logger.info("Stopping all consumers...")
    for consumer in consumers:
        consumer.close()

    # 等待一下，确保所有统计文件都写入完成
    time.sleep(2)

    # 聚合统计信息
    logger.info("Aggregating statistics...")
    aggregated = StatsAggregator.aggregate_consumer_stats(stats_dir, consumer_ids)

    # 打印结果
    StatsAggregator.print_aggregated_stats(aggregated)

    # 清理统计文件
    logger.info("Cleaning up stats files...")
    StatsAggregator.cleanup_stats_files(stats_dir, consumer_ids)

    logger.info("Test completed!")


if __name__ == '__main__':
    test_isolated_consumers()
