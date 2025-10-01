"""Benchmark coordinator for orchestrating distributed tests."""

import asyncio
import time
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
import aiohttp
from concurrent.futures import ThreadPoolExecutor

from .config import WorkloadConfig, DriverConfig, BenchmarkConfig
from .results import BenchmarkResult, WorkerResult, ResultCollector
from .monitoring import SystemMonitor
from .worker import ProducerTask, ConsumerTask
from ..utils.logging import LoggerMixin
from ..utils.config_validator import validate_driver_config, validate_workload_config, print_validation_warnings


class BenchmarkCoordinator(LoggerMixin):
    """Coordinates distributed benchmark execution across multiple workers."""

    def __init__(self, config: BenchmarkConfig):
        super().__init__()
        self.config = config
        self.result_collector = ResultCollector()
        self.system_monitor = SystemMonitor()
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        """Async context manager entry."""
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=600)  # 10 minute timeout
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._session:
            await self._session.close()

    async def run_benchmark(
        self,
        workload_config: WorkloadConfig,
        driver_config: DriverConfig,
        payload_data: Optional[bytes] = None
    ) -> BenchmarkResult:
        """Run a complete benchmark test.

        Args:
            workload_config: Workload configuration
            driver_config: Driver configuration
            payload_data: Optional payload data for messages

        Returns:
            Complete benchmark result
        """
        # Generate unique test ID and topic names (OMB style)
        import time
        test_timestamp = int(time.time())
        # Clean workload name for use in topic names (Kafka topic name restrictions)
        import re
        clean_name = re.sub(r'[^A-Za-z0-9._-]', '_', workload_config.name)
        unique_test_id = f"{clean_name}_{test_timestamp}"

        # Create unique topic names for this test
        unique_topics = []
        for topic_idx in range(workload_config.topics):
            unique_topic = f"benchmark-{unique_test_id}-topic-{topic_idx}"
            unique_topics.append(unique_topic)

        # Validate configurations before starting
        driver_warnings = validate_driver_config(driver_config)
        workload_warnings = validate_workload_config(workload_config)

        all_warnings = driver_warnings + workload_warnings
        if all_warnings:
            print_validation_warnings(all_warnings, self.logger)

        self.logger.info(f"🚀 Starting benchmark: {workload_config.name}")
        self.logger.info(f"🎯 Test ID: {unique_test_id}")
        self.logger.info(f"📋 Unique topics: {unique_topics}")

        # Store unique topics for use in tasks
        self._current_test_topics = unique_topics

        # Create benchmark result with unique ID
        result = self.result_collector.create_result(
            test_name=workload_config.name,
            workload_config=workload_config.dict(),
            driver_config=driver_config.dict()
        )
        result.test_id = unique_test_id

        try:
            # Check worker health
            await self._check_worker_health()

            # Setup topics for idempotent testing
            self.logger.info("🔧 Setting up topics...")
            await self._setup_topics(workload_config, driver_config)
            self.logger.info("✅ Topic setup completed")

            # Start system monitoring
            if self.config.enable_monitoring:
                self.system_monitor.start()

            # Warmup phase
            if self.config.warmup_enabled and workload_config.warmup_duration_minutes > 0:
                self.logger.info(f"🔥 Starting warmup phase ({workload_config.warmup_duration_minutes} minutes)...")
                await self._run_warmup_phase(workload_config, driver_config, payload_data)
                self.logger.info("✅ Warmup phase completed")

            # Main test phase
            self.logger.info(f"🚀 Starting main test phase ({workload_config.test_duration_minutes} minutes)...")
            await self._run_test_phase(result, workload_config, driver_config, payload_data)
            self.logger.info("✅ Main test phase completed")

            # Finalize results
            system_stats = None
            if self.config.enable_monitoring:
                self.system_monitor.stop()
                system_stats = self.system_monitor.get_stats()

            self.result_collector.finalize_result(result, system_stats)

            # Clean up topics after test (OMB style)
            await self._cleanup_test_topics(driver_config)

            self.logger.info(f"Benchmark completed: {workload_config.name}")
            return result

        except Exception as e:
            self.logger.error(f"Benchmark failed: {e}")
            result.end_time = time.time()
            result.metadata['error'] = str(e)
            raise

    async def _check_worker_health(self) -> None:
        """Check health of all workers."""
        self.logger.info("Checking worker health...")

        tasks = []
        for worker_url in self.config.workers:
            tasks.append(self._check_single_worker_health(worker_url))

        health_results = await asyncio.gather(*tasks, return_exceptions=True)

        healthy_workers = []
        for i, result in enumerate(health_results):
            worker_url = self.config.workers[i]
            if isinstance(result, Exception):
                self.logger.error(f"Worker {worker_url} health check failed: {result}")
            else:
                healthy_workers.append(worker_url)
                self.logger.info(f"Worker {worker_url} is healthy")

        if not healthy_workers:
            raise RuntimeError("No healthy workers available")

        # Update config to only use healthy workers
        self.config.workers = healthy_workers
        self.logger.info(f"Using {len(healthy_workers)} healthy workers")

    async def _check_single_worker_health(self, worker_url: str) -> Dict[str, Any]:
        """Check health of a single worker."""
        url = f"{worker_url}/health"
        async with self._session.get(url) as response:
            response.raise_for_status()
            return await response.json()

    async def _run_warmup_phase(
        self,
        workload_config: WorkloadConfig,
        driver_config: DriverConfig,
        payload_data: Optional[bytes]
    ) -> None:
        """Run warmup phase with same configuration as main test.

        The warmup phase should use the same workload settings as the main test
        to properly warm up the system. Only the duration is shorter.
        """
        self.logger.info(f"Running warmup phase for {workload_config.warmup_duration_minutes} minutes")

        # Create warmup config with same settings but shorter duration
        warmup_config = WorkloadConfig(
            name=f"{workload_config.name}_warmup",
            topics=workload_config.topics,
            partitions_per_topic=workload_config.partitions_per_topic,
            message_size=workload_config.message_size,
            producers_per_topic=workload_config.producers_per_topic,  # Use same as main test
            producer_rate=workload_config.producer_rate,  # Use same rate to properly warm up
            subscriptions_per_topic=workload_config.subscriptions_per_topic,
            consumer_per_subscription=workload_config.consumer_per_subscription,
            test_duration_minutes=workload_config.warmup_duration_minutes,
            key_distributor=workload_config.key_distributor
        )

        # Run warmup (results are discarded but logged)
        warmup_result = self.result_collector.create_result(
            test_name=warmup_config.name,
            workload_config=warmup_config.dict(),
            driver_config=driver_config.dict()
        )

        await self._run_test_phase(warmup_result, warmup_config, driver_config, payload_data)

        # Log warmup results for diagnostic purposes
        if warmup_result.producer_stats:
            self.logger.info(
                f"Warmup completed - Producer throughput: "
                f"{warmup_result.producer_stats.messages_per_second:.0f} msg/s"
            )
        if warmup_result.consumer_stats:
            self.logger.info(
                f"Warmup completed - Consumer throughput: "
                f"{warmup_result.consumer_stats.messages_per_second:.0f} msg/s"
            )

    async def _run_test_phase(
        self,
        result: BenchmarkResult,
        workload_config: WorkloadConfig,
        driver_config: DriverConfig,
        payload_data: Optional[bytes]
    ) -> None:
        """Run main test phase with timeout protection."""
        self.logger.info("Starting main test phase")

        # Calculate timeout: test duration + warmup + buffer
        test_duration_seconds = workload_config.test_duration_minutes * 60
        timeout_seconds = test_duration_seconds + 180  # Add 3-minute buffer

        # Generate tasks
        producer_tasks = self._generate_producer_tasks(workload_config, payload_data)
        consumer_tasks = self._generate_consumer_tasks(workload_config)

        # Distribute tasks across workers
        producer_task_groups = self._distribute_tasks(producer_tasks, len(self.config.workers))
        consumer_task_groups = self._distribute_tasks(consumer_tasks, len(self.config.workers))

        # CRITICAL: Start consumer tasks first and wait for subscription
        # This ensures consumers are ready BEFORE producers start sending
        self.logger.info(f"📥 Starting consumer tasks on {len(self.config.workers)} workers...")
        consumer_futures = []
        for i, worker_url in enumerate(self.config.workers):
            if i < len(consumer_task_groups) and consumer_task_groups[i]:
                task_count = len(consumer_task_groups[i])
                self.logger.info(f"  🚀 Worker {worker_url}: {task_count} consumer tasks")
                future = asyncio.create_task(
                    self._run_worker_consumer_tasks(worker_url, consumer_task_groups[i])
                )
                consumer_futures.append(future)

        # OPTIMIZATION: Wait longer for consumers to subscribe and receive partition assignment
        # This prevents the timing issue where producer sends before consumer is ready
        self.logger.info("⏱️  Waiting for consumers to subscribe and receive partition assignment...")
        await asyncio.sleep(5)  # Increased from 2s to 5s
        self.logger.info("✅ Consumers should now be ready to receive messages")

        # Start producer tasks AFTER consumers are ready
        self.logger.info(f"📤 Starting producer tasks on {len(self.config.workers)} workers...")
        producer_futures = []
        for i, worker_url in enumerate(self.config.workers):
            if i < len(producer_task_groups) and producer_task_groups[i]:
                task_count = len(producer_task_groups[i])
                self.logger.info(f"  🚀 Worker {worker_url}: {task_count} producer tasks")
                future = asyncio.create_task(
                    self._run_worker_producer_tasks(worker_url, producer_task_groups[i])
                )
                producer_futures.append(future)

        # Wait for all tasks to complete WITH TIMEOUT
        self.logger.info(f"⏳ Waiting for {len(producer_futures)} producer task groups (timeout: {timeout_seconds}s)...")
        all_producer_results = []
        try:
            for i, future in enumerate(producer_futures):
                try:
                    self.logger.info(f"  ⏳ Waiting for producer task group {i+1}/{len(producer_futures)}...")
                    # Add timeout for each task group
                    results = await asyncio.wait_for(future, timeout=timeout_seconds)
                    all_producer_results.extend(results)
                    self.logger.info(f"  ✅ Producer task group {i+1}/{len(producer_futures)} completed with {len(results)} results")
                except asyncio.TimeoutError:
                    self.logger.error(f"❌ Producer task group {i+1} timed out after {timeout_seconds}s")
                except Exception as e:
                    import traceback
                    self.logger.error(f"❌ Producer task group {i+1} failed: {e}")
                    self.logger.error(f"Producer task traceback: {traceback.format_exc()}")
        except Exception as e:
            self.logger.error(f"❌ Producer tasks failed: {e}")

        self.logger.info(f"⏳ Waiting for {len(consumer_futures)} consumer task groups (timeout: {timeout_seconds}s)...")
        all_consumer_results = []
        try:
            for i, future in enumerate(consumer_futures):
                try:
                    self.logger.info(f"  ⏳ Waiting for consumer task group {i+1}/{len(consumer_futures)}...")
                    # Add timeout for each task group
                    results = await asyncio.wait_for(future, timeout=timeout_seconds)
                    all_consumer_results.extend(results)
                    self.logger.info(f"  ✅ Consumer task group {i+1}/{len(consumer_futures)} completed with {len(results)} results")
                except asyncio.TimeoutError:
                    self.logger.error(f"❌ Consumer task group {i+1} timed out after {timeout_seconds}s")
                except Exception as e:
                    self.logger.error(f"❌ Consumer task group {i+1} failed: {e}")
        except Exception as e:
            self.logger.error(f"❌ Consumer tasks failed: {e}")

        # Add results to benchmark result
        for worker_result in all_producer_results:
            self.result_collector.add_worker_result(result, worker_result)

        for worker_result in all_consumer_results:
            self.result_collector.add_worker_result(result, worker_result)

        self.logger.info(f"Test phase completed. Producers: {len(all_producer_results)}, Consumers: {len(all_consumer_results)}")

    def _generate_producer_tasks(
        self,
        workload_config: WorkloadConfig,
        payload_data: Optional[bytes]
    ) -> List[ProducerTask]:
        """Generate producer tasks based on workload configuration."""
        tasks = []
        task_id = 0

        for topic_idx in range(workload_config.topics):
            topic_name = self._current_test_topics[topic_idx]

            for producer_idx in range(workload_config.producers_per_topic):
                # Calculate messages per producer
                messages_per_producer = (
                    workload_config.producer_rate *
                    workload_config.test_duration_minutes * 60
                ) // workload_config.producers_per_topic

                task = ProducerTask(
                    task_id=f"producer-{task_id}",
                    topic=topic_name,
                    num_messages=messages_per_producer,
                    message_size=workload_config.message_size,
                    rate_limit=workload_config.producer_rate // workload_config.producers_per_topic,
                    payload_data=payload_data,
                    key_pattern=workload_config.key_distributor
                )
                tasks.append(task)
                task_id += 1

        return tasks

    def _generate_consumer_tasks(self, workload_config: WorkloadConfig) -> List[ConsumerTask]:
        """Generate consumer tasks based on workload configuration."""
        tasks = []
        task_id = 0

        # Extract test_id from first topic name for unique subscription names
        # Topic names are like: "benchmark-{test_name}_{test_id}-topic-{idx}"
        test_id_suffix = ""
        if self._current_test_topics:
            # Extract test_id from topic name (e.g., "..._1759202250-topic-0" -> "1759202250")
            topic_parts = self._current_test_topics[0].split('-')
            for i, part in enumerate(topic_parts):
                if 'topic' in part and i > 0:
                    test_id_suffix = f"-{topic_parts[i-1]}"
                    break

        for topic_idx in range(workload_config.topics):
            topic_name = self._current_test_topics[topic_idx]

            for sub_idx in range(workload_config.subscriptions_per_topic):
                for consumer_idx in range(workload_config.consumer_per_subscription):
                    # Use unique subscription name per test to avoid offset conflicts
                    subscription_name = f"subscription-{sub_idx}{test_id_suffix}"

                    # Consumer需要运行比Producer更长的时间才能接收所有消息
                    # 额外时间包括：启动等待(5s) + producer运行时间 + 消息消费缓冲(60s)
                    producer_duration = workload_config.test_duration_minutes * 60
                    consumer_duration = producer_duration + 65  # 额外65秒确保接收完所有消息

                    task = ConsumerTask(
                        task_id=f"consumer-{task_id}",
                        topics=[topic_name],
                        subscription_name=subscription_name,
                        test_duration_seconds=consumer_duration
                    )
                    tasks.append(task)
                    task_id += 1

        return tasks

    def _distribute_tasks(self, tasks: List[Any], num_workers: int) -> List[List[Any]]:
        """Distribute tasks evenly across workers."""
        if not tasks:
            return [[] for _ in range(num_workers)]

        task_groups = [[] for _ in range(num_workers)]
        for i, task in enumerate(tasks):
            worker_idx = i % num_workers
            task_groups[worker_idx].append(task)

        return task_groups

    async def _run_worker_producer_tasks(
        self,
        worker_url: str,
        tasks: List[ProducerTask]
    ) -> List[WorkerResult]:
        """Run producer tasks on a specific worker."""
        if not tasks:
            return []

        # Convert tasks to API format
        api_tasks = []
        for task in tasks:
            api_task = {
                "task_id": task.task_id,
                "topic": task.topic,
                "num_messages": task.num_messages,
                "message_size": task.message_size,
                "rate_limit": task.rate_limit,
                "key_pattern": task.key_pattern,
                "properties": task.properties
            }

            if task.payload_data:
                import base64
                api_task["payload_data"] = base64.b64encode(task.payload_data).decode()

            api_tasks.append(api_task)

        url = f"{worker_url}/producer/start"
        async with self._session.post(url, json=api_tasks) as response:
            response.raise_for_status()
            result_data = await response.json()

        # Convert API results back to WorkerResult objects
        worker_results = []
        for result_dict in result_data["results"]:
            worker_result = self._deserialize_worker_result(result_dict, worker_url)
            worker_results.append(worker_result)

        return worker_results

    async def _run_worker_consumer_tasks(
        self,
        worker_url: str,
        tasks: List[ConsumerTask]
    ) -> List[WorkerResult]:
        """Run consumer tasks on a specific worker."""
        if not tasks:
            return []

        # Convert tasks to API format
        api_tasks = []
        for task in tasks:
            api_task = {
                "task_id": task.task_id,
                "topics": task.topics,
                "subscription_name": task.subscription_name,
                "test_duration_seconds": task.test_duration_seconds,
                "properties": task.properties
            }
            api_tasks.append(api_task)

        url = f"{worker_url}/consumer/start"
        async with self._session.post(url, json=api_tasks) as response:
            response.raise_for_status()
            result_data = await response.json()

        # Convert API results back to WorkerResult objects
        worker_results = []
        for result_dict in result_data["results"]:
            worker_result = self._deserialize_worker_result(result_dict, worker_url)
            worker_results.append(worker_result)

        return worker_results

    def _deserialize_worker_result(self, result_dict: Dict[str, Any], worker_url: str) -> WorkerResult:
        """Deserialize worker result from API response."""
        from .results import ThroughputStats, LatencyStats, ErrorStats

        try:
            throughput = ThroughputStats(**result_dict["throughput"])
            latency = LatencyStats(**result_dict["latency"])
            errors = ErrorStats(**result_dict["errors"])
        except Exception as e:
            self.logger.error(f"Failed to deserialize stats: {e}")
            self.logger.error(f"Result dict: {result_dict}")
            raise

        worker_result = WorkerResult(
            worker_id=result_dict["worker_id"],
            worker_url=worker_url,
            task_type=result_dict["task_type"],
            start_time=result_dict["start_time"],
            end_time=result_dict["end_time"],
            throughput=throughput,
            latency=latency,
            errors=errors,
            metadata=result_dict.get("metadata", {})
        )

        return worker_result

    async def _setup_topics(self, workload_config: WorkloadConfig, driver_config: DriverConfig) -> None:
        """Setup topics for idempotent testing."""
        try:
            # Import the driver dynamically
            module_path, class_name = driver_config.driver_class.rsplit('.', 1)
            module = __import__(module_path, fromlist=[class_name])
            driver_class = getattr(module, class_name)

            # Create driver instance
            driver = driver_class(driver_config)

            # Initialize driver
            await driver.initialize()

            try:
                # Create topic manager
                topic_manager = driver.create_topic_manager()

                # Create unique topics for this test (OMB style)
                self.logger.info("  🔧 Creating unique topics...")
                for topic_name in self._current_test_topics:
                    self.logger.info(f"    📋 Creating {topic_name} (partitions: {workload_config.partitions_per_topic})")

                    try:
                        await topic_manager.create_topic(
                            topic_name=topic_name,
                            partitions=workload_config.partitions_per_topic,
                            replication_factor=driver_config.replication_factor,
                            config=driver_config.topic_config
                        )
                        self.logger.info(f"    ✅ {topic_name} created")
                    except Exception as e:
                        self.logger.error(f"    ❌ Failed to create {topic_name}: {e}")
                        raise

                # Close topic manager
                await topic_manager.close()

            finally:
                # Clean up driver
                await driver.cleanup()

            self.logger.info(f"✅ Successfully setup {workload_config.topics} topics")

        except Exception as e:
            self.logger.error(f"❌ Failed to setup topics: {e}")
            raise


    async def _cleanup_test_topics(self, driver_config: DriverConfig) -> None:
        """Clean up test topics after benchmark with improved reliability.

        This method is called automatically after each test completes.
        Unique topic names prevent conflicts between consecutive tests.
        """
        if not hasattr(self, '_current_test_topics') or not self._current_test_topics:
            self.logger.debug("No topics to clean up")
            return

        topic_count = len(self._current_test_topics)
        self.logger.info(f"🧹 Cleaning up {topic_count} test topic(s)...")

        # Wait a bit for messages to settle before cleanup
        self.logger.info("⏱️  Waiting 5s for messages to settle...")
        await asyncio.sleep(5)

        try:
            # Import the driver dynamically
            module_path, class_name = driver_config.driver_class.rsplit('.', 1)
            module = __import__(module_path, fromlist=[class_name])
            driver_class = getattr(module, class_name)

            # Create driver instance
            driver = driver_class(driver_config)
            await driver.initialize()

            try:
                # Create topic manager
                topic_manager = driver.create_topic_manager()

                # Delete all test topics with timeout
                deleted_count = 0
                failed_topics = []

                for topic_name in self._current_test_topics:
                    try:
                        self.logger.debug(f"  Deleting {topic_name}")
                        # Add timeout for delete operation
                        await asyncio.wait_for(
                            topic_manager.delete_topic(topic_name),
                            timeout=30
                        )
                        deleted_count += 1
                    except asyncio.TimeoutError:
                        self.logger.warning(f"  Timeout deleting {topic_name}")
                        failed_topics.append(topic_name)
                    except Exception as e:
                        # Don't fail the test if cleanup fails
                        self.logger.warning(f"  Failed to delete {topic_name}: {e}")
                        failed_topics.append(topic_name)

                # Close topic manager
                await topic_manager.close()

                # Report cleanup results
                if deleted_count == topic_count:
                    self.logger.info(f"✅ Successfully deleted all {deleted_count} topic(s)")
                elif deleted_count > 0:
                    self.logger.warning(
                        f"⚠️  Partially cleaned up: {deleted_count}/{topic_count} topics deleted"
                    )
                    if failed_topics:
                        self.logger.warning(f"   Failed topics: {', '.join(failed_topics)}")
                        self.logger.warning(
                            f"   To manually delete: kafka-topics --bootstrap-server <server> "
                            f"--delete --topic <topic-name>"
                        )
                else:
                    self.logger.warning(f"⚠️  Failed to delete any topics")
                    if failed_topics:
                        self.logger.warning(f"   Failed topics: {', '.join(failed_topics)}")

            finally:
                # Cleanup driver
                await driver.cleanup()

        except Exception as e:
            # Don't fail the test if cleanup fails
            self.logger.warning(f"⚠️  Topic cleanup failed: {e}")
            self.logger.warning(
                f"   Please manually delete topics: {', '.join(self._current_test_topics)}"
            )

        finally:
            # Clear the topics list
            self._current_test_topics = []
