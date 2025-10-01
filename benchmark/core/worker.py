"""Base worker implementation for distributed benchmark execution."""

import asyncio
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, Future
from ..utils.logging import LoggerMixin
from .results import WorkerResult, ThroughputStats, LatencyStats, ErrorStats
from .monitoring import SystemMonitor


@dataclass
class ProducerTask:
    """Producer task definition - continuous run mode (Java OMB style).

    Producer runs continuously until stop signal is received.
    No fixed num_messages - controlled by test duration and rate limit.
    """
    task_id: str
    topic: str
    message_size: int
    rate_limit: int = 0  # Messages per second, 0 = unlimited
    payload_data: Optional[bytes] = None
    key_pattern: str = "NO_KEY"
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConsumerTask:
    """Consumer task definition - continuous run mode (Java OMB style).

    Consumer runs continuously until stop signal is received.
    No fixed test_duration - controlled by coordinator's stop signal.
    """
    task_id: str
    topics: List[str]
    subscription_name: str
    properties: Dict[str, Any] = field(default_factory=dict)


class BaseWorker(LoggerMixin, ABC):
    """Base worker for benchmark execution."""

    def __init__(self, worker_id: str):
        super().__init__()
        self.worker_id = worker_id
        self.system_monitor = SystemMonitor()
        self.executor = ThreadPoolExecutor(max_workers=10)
        self._running_tasks: Dict[str, Future] = {}
        self._task_results: Dict[str, WorkerResult] = {}
        self._consumer_ready_status: Dict[str, Dict[str, Any]] = {}  # Track consumer readiness

    async def start(self) -> None:
        """Start the worker."""
        self.logger.info(f"Worker {self.worker_id} starting")
        self.system_monitor.start()

    async def stop(self) -> None:
        """Stop the worker."""
        self.logger.info(f"Worker {self.worker_id} stopping")

        # Cancel all running tasks
        for task_id, future in self._running_tasks.items():
            if not future.done():
                future.cancel()
                self.logger.info(f"Cancelled task {task_id}")

        # Shutdown executor
        self.executor.shutdown(wait=True)

        # Stop system monitoring
        self.system_monitor.stop()

    async def run_producer_tasks(self, tasks: List[ProducerTask]) -> List[WorkerResult]:
        """Run producer tasks."""
        self.logger.info(f"Running {len(tasks)} producer tasks")

        # Run tasks concurrently using asyncio
        tasks_to_run = [self._run_producer_task(task) for task in tasks]
        results = await asyncio.gather(*tasks_to_run, return_exceptions=True)

        # Process results and handle exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"Producer task {tasks[i].task_id} failed: {result}")
                # Create error result
                error_result = WorkerResult(
                    worker_id=self.worker_id,
                    worker_url="",  # Will be set by API
                    task_type="producer",
                    start_time=time.time(),
                    end_time=time.time()
                )
                error_result.errors.total_errors = 1
                error_result.errors.error_types["task_execution"] = 1
                processed_results.append(error_result)
            else:
                self.logger.info(f"Producer task {tasks[i].task_id} completed")
                processed_results.append(result)

        return processed_results

    async def run_consumer_tasks(self, tasks: List[ConsumerTask]) -> List[WorkerResult]:
        """Run consumer tasks."""
        self.logger.info(f"Running {len(tasks)} consumer tasks")

        # Run tasks concurrently using asyncio
        tasks_to_run = [self._run_consumer_task(task) for task in tasks]
        results = await asyncio.gather(*tasks_to_run, return_exceptions=True)

        # Process results and handle exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"Consumer task {tasks[i].task_id} failed: {result}")
                # Create error result
                error_result = WorkerResult(
                    worker_id=self.worker_id,
                    worker_url="",  # Will be set by API
                    task_type="consumer",
                    start_time=time.time(),
                    end_time=time.time()
                )
                error_result.errors.total_errors = 1
                error_result.errors.error_types["task_execution"] = 1
                processed_results.append(error_result)
            else:
                self.logger.info(f"Consumer task {tasks[i].task_id} completed")
                processed_results.append(result)

        return processed_results

    async def _run_producer_task(self, task: ProducerTask) -> WorkerResult:
        """Run a single producer task."""
        start_time = time.time()

        result = WorkerResult(
            worker_id=self.worker_id,
            worker_url="",  # Will be set by API
            task_type="producer",
            start_time=start_time,
            end_time=0.0
        )

        try:
            # Execute producer task implementation
            if asyncio.iscoroutinefunction(self._execute_producer_task):
                stats = await self._execute_producer_task(task)
            else:
                stats = self._execute_producer_task(task)
            result.throughput = stats['throughput']
            result.latency = stats['latency']
            result.errors = stats['errors']

        except Exception as e:
            self.logger.error(f"Producer task execution failed: {e}")
            result.errors.total_errors = 1
            result.errors.error_types[str(type(e).__name__)] = 1

        result.end_time = time.time()
        self._task_results[task.task_id] = result
        return result

    async def _run_consumer_task(self, task: ConsumerTask) -> WorkerResult:
        """Run a single consumer task."""
        start_time = time.time()

        result = WorkerResult(
            worker_id=self.worker_id,
            worker_url="",  # Will be set by API
            task_type="consumer",
            start_time=start_time,
            end_time=0.0
        )

        try:
            # Execute consumer task implementation
            if asyncio.iscoroutinefunction(self._execute_consumer_task):
                stats = await self._execute_consumer_task(task)
            else:
                stats = self._execute_consumer_task(task)
            result.throughput = stats['throughput']
            result.errors = stats['errors']

        except Exception as e:
            self.logger.error(f"Consumer task execution failed: {e}")
            result.errors.total_errors = 1
            result.errors.error_types[str(type(e).__name__)] = 1

        result.end_time = time.time()
        self._task_results[task.task_id] = result
        return result

    @abstractmethod
    def _execute_producer_task(self, task: ProducerTask) -> Dict[str, Any]:
        """Execute producer task implementation.

        Returns:
            Dict containing 'throughput', 'latency', and 'errors' stats
        """
        pass

    @abstractmethod
    def _execute_consumer_task(self, task: ConsumerTask) -> Dict[str, Any]:
        """Execute consumer task implementation.

        Returns:
            Dict containing 'throughput' and 'errors' stats
        """
        pass

    def get_system_stats(self) -> Dict[str, Any]:
        """Get current system statistics."""
        stats = self.system_monitor.get_stats()
        return {
            'cpu': stats.cpu,
            'memory': stats.memory,
            'network': stats.network,
            'disk': stats.disk
        }

    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """Get status of a specific task."""
        if task_id in self._running_tasks:
            future = self._running_tasks[task_id]
            status = "running" if not future.done() else "completed"
        else:
            status = "not_found"

        return {
            'task_id': task_id,
            'status': status,
            'result_available': task_id in self._task_results
        }

    def get_task_result(self, task_id: str) -> Optional[WorkerResult]:
        """Get result of a completed task."""
        return self._task_results.get(task_id)

    def get_consumer_ready_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get consumer readiness status for a specific task."""
        return self._consumer_ready_status.get(task_id)

    def health_check(self) -> Dict[str, Any]:
        """Perform health check."""
        return {
            'worker_id': self.worker_id,
            'status': 'healthy',
            'running_tasks': len([f for f in self._running_tasks.values() if not f.done()]),
            'completed_tasks': len(self._task_results),
            'system_stats': self.get_system_stats()
        }