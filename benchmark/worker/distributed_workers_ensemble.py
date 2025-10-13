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

    def start_load(self, producer_work_assignment: ProducerWorkAssignment):
        """Start load on all workers."""
        self._execute_on_all_workers('start_load', producer_work_assignment)

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
