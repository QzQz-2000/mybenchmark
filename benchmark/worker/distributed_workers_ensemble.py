import logging
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
from .worker import Worker
from .commands.consumer_assignment import ConsumerAssignment
from .commands.counters_stats import CountersStats
from .commands.cumulative_latencies import CumulativeLatencies
from .commands.period_stats import PeriodStats
from .commands.producer_work_assignment import ProducerWorkAssignment
from .commands.topics_info import TopicsInfo

logger = logging.getLogger(__name__)


class DistributedWorkersEnsemble(Worker):
    """
    Ensemble of distributed workers.
    Coordinates multiple remote workers to run benchmarks in parallel.
    """

    def __init__(self, workers: List[Worker], extra_consumers: bool = False):
        """
        Initialize distributed workers ensemble.

        :param workers: List of Worker instances (usually HttpWorkerClient)
        :param extra_consumers: Whether to allocate extra consumer workers
        """
        self.workers = workers
        self.extra_consumers = extra_consumers
        # 只需要 len(workers) 个线程，每个worker一个线程即可
        self.executor = ThreadPoolExecutor(max_workers=max(1, len(workers)), thread_name_prefix="distributed-worker")

        # V2架构标志：consumer_metadata表示使用独立Consumer进程
        # 在分布式模式下，每个远程Worker都使用V2架构，所以设置此标志跳过probe阶段
        self.consumer_metadata = []  # 标记为V2架构，跳过probe phase

        # V2架构使用的事件（在分布式模式下通过HTTP API传播到远程Workers）
        # 使用自定义类来包装HTTP调用
        class DistributedEvent:
            """分布式事件，通过HTTP API传播到所有Workers"""
            def __init__(self, ensemble):
                self._ensemble = ensemble

            def set(self):
                """设置事件 - 通知所有Workers开始生产"""
                futures = []
                for worker in self._ensemble.workers:
                    if hasattr(worker, 'start_producing'):
                        future = self._ensemble.executor.submit(worker.start_producing)
                        futures.append(future)
                # 等待所有Workers确认
                for future in futures:
                    future.result()

            def clear(self):
                """清除事件 - 通知所有Workers停止生产"""
                futures = []
                for worker in self._ensemble.workers:
                    if hasattr(worker, 'stop_producing'):
                        future = self._ensemble.executor.submit(worker.stop_producing)
                        futures.append(future)
                # 等待所有Workers确认
                for future in futures:
                    future.result()

        self.start_producing_event = DistributedEvent(self)

        # 添加 benchmark_driver 属性（用于兼容 workload_generator 的 topic 删除逻辑）
        # 实际上是第一个 worker 的 driver（因为 topic 操作只需要在一个 worker 上执行）
        class DistributedDriver:
            """分布式 Driver 包装器，将操作委托给第一个 Worker"""
            def __init__(self, ensemble):
                self._ensemble = ensemble

            def delete_topics(self, topics: List[str]):
                """删除 topics - 只在第一个 worker 上执行"""
                if self._ensemble.workers:
                    self._ensemble.workers[0].delete_topics(topics)
                    # 返回一个 Future 对象以兼容原有接口
                    from concurrent.futures import Future
                    future = Future()
                    future.set_result(None)
                    return future
                else:
                    raise RuntimeError("No workers available")

            def get_topic_name_prefix(self):
                """获取 topic 名称前缀 - 从第一个 worker 获取"""
                # 默认返回标准前缀，因为所有 workers 应该使用相同的前缀
                return "test-topic"

        self.benchmark_driver = DistributedDriver(self)

    def _execute_on_all_workers(self, func_name: str, *args, **kwargs):
        """
        Execute a function on all workers in parallel.

        :param func_name: Name of the function to call on each worker
        :param args: Positional arguments
        :param kwargs: Keyword arguments
        """
        futures = []
        for worker in self.workers:
            func = getattr(worker, func_name)
            future = self.executor.submit(func, *args, **kwargs)
            futures.append(future)

        # Wait for all to complete and collect results
        results = []
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Error executing {func_name} on worker: {e}")
                raise

        return results

    def initialize_driver(self, configuration_file: str):
        """Initialize driver on all workers."""
        self._execute_on_all_workers('initialize_driver', configuration_file)

    def create_topics(self, topics_info: TopicsInfo) -> List[str]:
        """Create topics (only on first worker to avoid duplicates)."""
        # Only create on first worker
        return self.workers[0].create_topics(topics_info)

    def create_producers(self, topics: List[str]):
        """Create producers distributed across workers."""
        # Partition topics across workers
        topics_per_worker = self._partition_list(topics, len(self.workers))

        logger.info(f"📊 Distributing {len(topics)} producer topics across {len(self.workers)} workers:")
        for i, worker_topics in enumerate(topics_per_worker):
            if worker_topics:
                logger.info(f"  Worker {i+1} ({self.workers[i].id()}): {len(worker_topics)} topics → {worker_topics}")

        futures = []
        for i, worker in enumerate(self.workers):
            worker_topics = topics_per_worker[i]
            if worker_topics:
                future = self.executor.submit(worker.create_producers, worker_topics)
                futures.append(future)

        # Wait for all
        for future in as_completed(futures):
            future.result()

    def create_consumers(self, consumer_assignment: ConsumerAssignment):
        """Create consumers distributed across workers."""
        # Partition subscriptions across workers
        subscriptions = consumer_assignment.topics_subscriptions
        subs_per_worker = self._partition_list(subscriptions, len(self.workers))

        # 更新consumer_metadata以标记V2架构（用于跳过probe phase）
        self.consumer_metadata = subscriptions

        logger.info(f"📊 Distributing {len(subscriptions)} consumer subscriptions across {len(self.workers)} workers:")
        for i, worker_subs in enumerate(subs_per_worker):
            if worker_subs:
                worker_topics = [ts.topic for ts in worker_subs]
                logger.info(f"  Worker {i+1} ({self.workers[i].id()}): {len(worker_subs)} subscriptions → {worker_topics}")

        futures = []
        for i, worker in enumerate(self.workers):
            worker_assignment = ConsumerAssignment()
            worker_assignment.topics_subscriptions = subs_per_worker[i]

            if worker_assignment.topics_subscriptions:
                future = self.executor.submit(worker.create_consumers, worker_assignment)
                futures.append(future)

        # Wait for all
        for future in as_completed(futures):
            future.result()

    def probe_producers(self):
        """Probe producers on all workers."""
        self._execute_on_all_workers('probe_producers')

    def start_load(self, producer_work_assignment: ProducerWorkAssignment, message_processing_delay_ms: int = 0):
        """Start load on all workers."""
        self._execute_on_all_workers('start_load', producer_work_assignment, message_processing_delay_ms)

    def adjust_publish_rate(self, publish_rate: float):
        """Adjust publish rate on all workers."""
        self._execute_on_all_workers('adjust_publish_rate', publish_rate)

    def pause_consumers(self):
        """Pause consumers on all workers."""
        self._execute_on_all_workers('pause_consumers')

    def resume_consumers(self):
        """Resume consumers on all workers."""
        self._execute_on_all_workers('resume_consumers')

    def get_counters_stats(self) -> CountersStats:
        """Get aggregated counter stats from all workers."""
        all_stats = self._execute_on_all_workers('get_counters_stats')

        # Aggregate stats
        aggregated = CountersStats()
        for stats in all_stats:
            aggregated = aggregated.plus(stats)

        return aggregated

    def get_period_stats(self) -> PeriodStats:
        """Get aggregated period stats from all workers."""
        all_stats = self._execute_on_all_workers('get_period_stats')

        # Aggregate stats
        aggregated = all_stats[0] if all_stats else PeriodStats()
        for stats in all_stats[1:]:
            aggregated = aggregated.plus(stats)

        return aggregated

    def get_cumulative_latencies(self) -> CumulativeLatencies:
        """Get aggregated cumulative latencies from all workers."""
        all_latencies = self._execute_on_all_workers('get_cumulative_latencies')

        # Aggregate latencies
        aggregated = all_latencies[0] if all_latencies else CumulativeLatencies()
        for latencies in all_latencies[1:]:
            aggregated = aggregated.plus(latencies)

        return aggregated

    def reset_stats(self):
        """Reset stats on all workers."""
        self._execute_on_all_workers('reset_stats')

    def stop_all(self):
        """Stop all workers."""
        self._execute_on_all_workers('stop_all')

    def id(self) -> str:
        """Get ensemble ID."""
        worker_ids = [w.id() for w in self.workers]
        return f"ensemble({', '.join(worker_ids)})"

    def close(self):
        """Close all workers."""
        for worker in self.workers:
            try:
                worker.close()
            except Exception as e:
                logger.error(f"Error closing worker: {e}")

        self.executor.shutdown(wait=True)

    @staticmethod
    def _partition_list(items: List, num_partitions: int) -> List[List]:
        """
        Partition a list into num_partitions sublists.

        :param items: List to partition
        :param num_partitions: Number of partitions
        :return: List of sublists
        """
        if not items or num_partitions <= 0:
            return [[] for _ in range(num_partitions)]

        result = [[] for _ in range(num_partitions)]

        if len(items) <= num_partitions:
            # Each item gets its own partition
            for i, item in enumerate(items):
                result[i].append(item)
        else:
            # Distribute items round-robin
            for i, item in enumerate(items):
                result[i % num_partitions].append(item)

        return result
