"""Kafka worker implementation for distributed benchmark execution."""

import time
import asyncio
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor

from benchmark.core.worker import BaseWorker, ProducerTask, ConsumerTask
from benchmark.core.results import ThroughputStats, LatencyStats, ErrorStats
from benchmark.drivers.kafka import KafkaDriver
from benchmark.drivers.base import Message, DriverUtils
from benchmark.core.config import DriverConfig
from benchmark.utils.logging import LoggerMixin


class KafkaWorker(BaseWorker):
    """Kafka-specific worker implementation."""

    def __init__(
        self,
        worker_id: str,
        driver_config: DriverConfig,
        use_multiprocessing: bool = True,
        num_processes: int = 4
    ):
        """Initialize Kafka worker for Digital Twin scenarios.

        Args:
            worker_id: Unique worker identifier
            driver_config: Kafka driver configuration
            use_multiprocessing: Enable multiprocessing for high throughput (default: True)
            num_processes: Number of sender processes when multiprocessing enabled (default: 4)
        """
        super().__init__(worker_id)
        self.driver_config = driver_config
        self.client_type = "confluent-kafka"
        self._driver: Optional[KafkaDriver] = None

        # Multiprocessing configuration
        self.use_multiprocessing = use_multiprocessing
        self.num_processes = num_processes

        # Note: Connection pool removed - each task creates its own connection
        # This is simpler and more reliable than pooling

    async def start(self) -> None:
        """Start the Kafka worker."""
        await super().start()

        # Initialize Kafka driver
        self._driver = KafkaDriver(self.driver_config)
        await self._driver.initialize()

        self.logger.info(f"Kafka worker {self.worker_id} started for Digital Twin scenarios")

    async def stop(self) -> None:
        """Stop the Kafka worker."""
        if self._driver:
            await self._driver.cleanup()

        await super().stop()

    async def _execute_producer_task(self, task: ProducerTask) -> Dict[str, Any]:
        """Execute Kafka producer task optimized for Digital Twin scenarios."""
        self.logger.info(f"Executing Digital Twin producer task: {task.task_id}")

        # Initialize metrics
        errors = {}
        start_time = time.time()

        # Create a new producer for this task (simpler and more reliable than pooling)
        producer = self._driver.create_producer()
        await producer._initialize_producer()

        try:
            # Create payload optimized for Digital Twin data
            payload = DriverUtils.create_test_payload(task.message_size, task.payload_data)
            total_bytes = len(payload) * task.num_messages

            # Rate limiting setup if configured
            rate_limiter = None
            use_parallel = False

            print(f"[PRODUCER DEBUG] Task rate_limit: {task.rate_limit}, use_multiprocessing: {self.use_multiprocessing}, num_processes: {self.num_processes}", flush=True)
            self.logger.info(
                f"Task rate_limit: {task.rate_limit}, "
                f"use_multiprocessing: {self.use_multiprocessing}, "
                f"num_processes: {self.num_processes}"
            )

            if task.rate_limit and task.rate_limit > 0:
                # Use multiprocessing for high rate (>1500 msg/s)
                if self.use_multiprocessing and task.rate_limit > 1500:
                    use_parallel = True
                    print(f"[PRODUCER DEBUG] ✅ High rate detected ({task.rate_limit} msg/s), using {self.num_processes} parallel processes", flush=True)
                    self.logger.info(
                        f"✅ High rate detected ({task.rate_limit} msg/s), "
                        f"using {self.num_processes} parallel processes"
                    )
                else:
                    from benchmark.utils.rate_limiter import create_rate_limiter
                    rate_limiter = create_rate_limiter(task.rate_limit)
                    self.logger.info(f"Rate limiting enabled: {task.rate_limit} msg/s (single process)")

            # HIGH THROUGHPUT PATH: Use multiprocessing for rates > 1500 msg/s
            if use_parallel:
                from benchmark.utils.parallel_sender import ParallelSender

                sender = ParallelSender(num_processes=self.num_processes)

                # Extract Kafka config
                bootstrap_servers = self.driver_config.common_config.get(
                    'bootstrap.servers', 'localhost:9092'
                )

                # Convert producer config to dict (merge common + producer config)
                producer_config_dict = {}
                # Add common config first
                for key, value in self.driver_config.common_config.items():
                    if key != 'bootstrap.servers':  # Skip bootstrap.servers, passed separately
                        producer_config_dict[key] = str(value)
                # Add producer-specific config (overrides common if same key)
                for key, value in self.driver_config.producer_config.items():
                    producer_config_dict[key] = str(value)

                # Send messages in parallel
                result = await sender.send_parallel(
                    topic=task.topic,
                    num_messages=task.num_messages,
                    message_size=task.message_size,
                    payload=payload,
                    rate_per_second=task.rate_limit,
                    bootstrap_servers=bootstrap_servers,
                    producer_config=producer_config_dict
                )

                sender.cleanup()

                # Build statistics from parallel result
                messages_sent = result['total_messages']
                actual_total_bytes = result['total_bytes']
                end_time = time.time()

                # Calculate throughput
                throughput_stats = DriverUtils.calculate_throughput_stats(
                    total_messages=messages_sent,
                    total_bytes=actual_total_bytes,
                    start_time=start_time,
                    end_time=end_time
                )

                # Use pre-calculated latency stats from parallel sender
                # This avoids double-processing and uses all latency samples
                latency_stats_dict = result.get('latency_stats')
                if latency_stats_dict:
                    from benchmark.core.results import LatencyStats
                    latency_stats = LatencyStats(
                        min_ms=latency_stats_dict['min_ms'],
                        max_ms=latency_stats_dict['max_ms'],
                        mean_ms=latency_stats_dict['mean_ms'],
                        median_ms=latency_stats_dict['median_ms'],
                        p50_ms=latency_stats_dict['p50_ms'],
                        p95_ms=latency_stats_dict['p95_ms'],
                        p99_ms=latency_stats_dict['p99_ms'],
                        p99_9_ms=latency_stats_dict['p99_9_ms']
                    )
                    self.logger.info(
                        f"Latency stats: p50={latency_stats.p50_ms:.2f}ms, "
                        f"p99={latency_stats.p99_ms:.2f}ms, "
                        f"samples={latency_stats_dict['count']}"
                    )
                else:
                    latency_stats = None

                error_stats = DriverUtils.create_error_stats({})
                error_stats.total_errors = result['total_errors']

                self.logger.info(
                    f"Parallel producer completed: {messages_sent} messages "
                    f"using {self.num_processes} processes, "
                    f"throughput: {throughput_stats.messages_per_second:.2f} msg/s"
                )

                return {
                    'throughput': throughput_stats,
                    'latency': latency_stats,
                    'errors': error_stats
                }

            # SINGLE PROCESS PATH: Original OMB design for low/medium rates
            elif rate_limiter:
                # OMB-style: Single message sending with rate control
                for i in range(task.num_messages):
                    # Acquire rate limit permit and wait until intended send time
                    # This matches original OMB's MessageProducer.sendMessage() behavior
                    await rate_limiter.acquire()

                    # Create message with Digital Twin optimizations
                    key = DriverUtils.generate_message_key(
                        task.key_pattern, i, task.num_messages
                    )

                    # Add Digital Twin metadata to headers
                    # Include send timestamp for end-to-end latency measurement
                    send_timestamp_ms = int(time.time() * 1000)
                    headers = {
                        'dt_sensor_id': f'sensor_{i % 100}'.encode(),
                        'dt_timestamp': str(send_timestamp_ms).encode(),
                        'dt_batch_id': task.task_id.encode(),
                        'send_timestamp': str(send_timestamp_ms).encode()  # For E2E latency
                    }

                    message = Message(
                        key=key,
                        value=payload,
                        timestamp=time.time(),
                        headers=headers
                    )

                    try:
                        # Send single message immediately after rate limit check
                        # This ensures timing accuracy like original OMB
                        await producer.send_message(task.topic, message)
                    except Exception as e:
                        error_type = type(e).__name__
                        errors[error_type] = errors.get(error_type, 0) + 1
                        self.logger.warning(f"Message send failed: {e}")

            else:
                # No rate limiting: Use batch sending for maximum throughput
                batch_size = min(1000, task.num_messages // 10) if task.num_messages > 100 else task.num_messages
                messages_to_send = []

                for i in range(task.num_messages):
                    # Create message with Digital Twin optimizations
                    key = DriverUtils.generate_message_key(
                        task.key_pattern, i, task.num_messages
                    )

                    # Add Digital Twin metadata to headers
                    send_timestamp_ms = int(time.time() * 1000)
                    headers = {
                        'dt_sensor_id': f'sensor_{i % 100}'.encode(),
                        'dt_timestamp': str(send_timestamp_ms).encode(),
                        'dt_batch_id': task.task_id.encode(),
                        'send_timestamp': str(send_timestamp_ms).encode()
                    }

                    message = Message(
                        key=key,
                        value=payload,
                        timestamp=time.time(),
                        headers=headers
                    )
                    messages_to_send.append(message)

                    # Send in batches for better performance
                    if len(messages_to_send) >= batch_size or i == task.num_messages - 1:
                        try:
                            # Use batch sending for better performance
                            produced_messages = await producer.send_batch(task.topic, messages_to_send)
                        except Exception as e:
                            error_type = type(e).__name__
                            errors[error_type] = errors.get(error_type, 0) + len(messages_to_send)
                            self.logger.warning(f"Batch send failed: {e}")

                        messages_to_send.clear()

            # Ensure all messages are flushed for Digital Twin consistency
            await producer.flush()

            # CRITICAL: Wait for all delivery confirmations before getting stats
            # This ensures delivery callbacks have been processed and latencies recorded
            delivery_status = await producer.wait_for_delivery(timeout_seconds=60)
            if delivery_status.get('timed_out', False):
                self.logger.warning(
                    f"Delivery timeout for task {task.task_id}: "
                    f"{delivery_status.get('pending_messages', 0)} messages still pending"
                )

        except Exception as e:
            self.logger.error(f"Producer task failed: {e}")
            error_type = type(e).__name__
            errors[error_type] = errors.get(error_type, 0) + 1

        finally:
            # End time and get statistics
            end_time = time.time()

            # Get accurate latency statistics from HdrHistogram
            latency_snapshot = producer.get_latency_snapshot()
            messages_sent = latency_snapshot.count

            # Export histogram data for proper aggregation
            histogram_data = producer._latency_recorder.export_histogram()

            # Log actual messages sent vs expected
            if messages_sent != task.num_messages:
                self.logger.warning(
                    f"Message count mismatch: expected {task.num_messages}, "
                    f"got {messages_sent} in latency recorder"
                )

            # Always close the producer when done
            try:
                await producer.close()
            except Exception as e:
                self.logger.warning(f"Failed to close producer: {e}")

        actual_total_bytes = len(payload) * messages_sent if messages_sent > 0 else 0

        throughput_stats = DriverUtils.calculate_throughput_stats(
            total_messages=messages_sent,
            total_bytes=actual_total_bytes,
            start_time=start_time,
            end_time=end_time
        )

        # Convert snapshot to legacy LatencyStats format with histogram data
        from benchmark.utils.latency_recorder import snapshot_to_legacy_stats
        latency_stats = snapshot_to_legacy_stats(latency_snapshot, histogram_data)

        error_stats = DriverUtils.create_error_stats(errors)
        if messages_sent > 0:
            error_stats.error_rate = error_stats.total_errors / (
                messages_sent + error_stats.total_errors
            )

        self.logger.info(
            f"Digital Twin producer task completed: {messages_sent} messages, "
            f"mean latency: {latency_snapshot.mean_ms:.2f}ms, "
            f"p99: {latency_snapshot.p99_ms:.2f}ms"
        )

        return {
            'throughput': throughput_stats,
            'latency': latency_stats,
            'errors': error_stats
        }

    async def _execute_consumer_task(self, task: ConsumerTask) -> Dict[str, Any]:
        """Execute Kafka consumer task with end-to-end latency tracking."""
        self.logger.info(f"Executing consumer task: {task.task_id}")

        # Initialize metrics
        message_count = 0
        total_bytes = 0
        errors = {}
        start_time = time.time()
        end_time = start_time + task.test_duration_seconds

        # End-to-end latency tracking (like original OMB)
        from benchmark.utils.latency_recorder import EndToEndLatencyRecorder
        e2e_latency_recorder = EndToEndLatencyRecorder()

        # Create consumer with proper resource management
        consumer = self._driver.create_consumer()
        try:
            # Initialize consumer if needed
            if hasattr(consumer, 'initialize'):
                await consumer.initialize()
            # Subscribe to topics
            await consumer.subscribe(task.topics, task.subscription_name)
            print(f"[CONSUMER DEBUG] Subscribed to topics: {task.topics}, subscription: {task.subscription_name}, will run until {end_time - start_time:.1f}s", flush=True)

            # Consume messages
            while time.time() < end_time:
                try:
                    # Consume messages for 1 second
                    consumed_any = False
                    async for consumed_message in consumer.consume_messages(timeout_seconds=1.0):
                        message_count += 1
                        if message_count == 1 or message_count % 10000 == 0:
                            print(f"[CONSUMER DEBUG] Consumed {message_count} messages so far...", flush=True)

                        # Extract end-to-end latency from message headers
                        if hasattr(consumed_message, 'message') and hasattr(consumed_message.message, 'headers'):
                            headers = consumed_message.message.headers or {}
                            if 'send_timestamp' in headers:
                                try:
                                    send_ts_bytes = headers['send_timestamp']
                                    if isinstance(send_ts_bytes, bytes):
                                        send_timestamp_ms = float(send_ts_bytes.decode('utf-8'))
                                        e2e_latency_recorder.record_from_timestamp(send_timestamp_ms)
                                except Exception as e:
                                    pass  # Silently ignore malformed timestamps

                        # Safely get message value
                        if hasattr(consumed_message, 'message') and hasattr(consumed_message.message, 'value'):
                            message_value = consumed_message.message.value
                            if isinstance(message_value, (bytes, str)):
                                total_bytes += len(message_value)
                            else:
                                total_bytes += len(str(message_value).encode())
                        else:
                            # Fallback for unexpected message format
                            total_bytes += len(str(consumed_message).encode())
                        consumed_any = True

                    # If no messages, continue to next iteration
                    if not consumed_any:
                        await asyncio.sleep(0.1)

                except Exception as e:
                    error_type = type(e).__name__
                    errors[error_type] = errors.get(error_type, 0) + 1
                    self.logger.warning(f"Message consumption failed: {e}")

        except Exception as e:
            self.logger.error(f"Consumer task failed: {e}")
            error_type = type(e).__name__
            errors[error_type] = errors.get(error_type, 0) + 1

        finally:
            # Close consumer with proper error handling
            try:
                if hasattr(consumer, 'close'):
                    await consumer.close()
                elif hasattr(consumer, '_close'):
                    await consumer._close()
            except Exception as e:
                self.logger.warning(f"Failed to close consumer: {e}")

        actual_end_time = time.time()
        print(f"[CONSUMER DEBUG] Consumer task finished: consumed {message_count} messages in {actual_end_time - start_time:.1f}s", flush=True)

        # Calculate statistics
        throughput_stats = DriverUtils.calculate_throughput_stats(
            total_messages=message_count,
            total_bytes=total_bytes,
            start_time=start_time,
            end_time=actual_end_time
        )

        error_stats = DriverUtils.create_error_stats(errors)
        if message_count > 0:
            error_stats.error_rate = error_stats.total_errors / (
                message_count + error_stats.total_errors
            )

        # Get end-to-end latency stats
        e2e_latency_snapshot = e2e_latency_recorder.get_snapshot()
        from benchmark.utils.latency_recorder import snapshot_to_legacy_stats
        e2e_latency_stats = snapshot_to_legacy_stats(e2e_latency_snapshot)

        self.logger.info(
            f"Consumer task completed: {message_count} messages consumed, "
            f"E2E latency samples: {e2e_latency_snapshot.count}, "
            f"E2E mean: {e2e_latency_snapshot.mean_ms:.2f}ms, "
            f"E2E p99: {e2e_latency_snapshot.p99_ms:.2f}ms"
        )

        return {
            'throughput': throughput_stats,
            'latency': e2e_latency_stats,  # Include end-to-end latency
            'errors': error_stats
        }

    def _create_rate_limiter(self, rate_per_second: int):
        """Create an async rate limiter."""
        import asyncio
        import time

        class AsyncRateLimiter:
            def __init__(self, rate):
                self.rate = rate
                self.interval = 1.0 / rate  # Time between tokens
                self.last_token_time = time.time()
                self._lock = asyncio.Lock()

            async def acquire(self):
                async with self._lock:
                    now = time.time()
                    time_since_last = now - self.last_token_time

                    if time_since_last >= self.interval:
                        # Enough time has passed, grant the token
                        self.last_token_time = now
                        return True
                    else:
                        # Need to wait
                        sleep_time = self.interval - time_since_last
                        await asyncio.sleep(sleep_time)
                        self.last_token_time = time.time()
                        return True

        return AsyncRateLimiter(rate_per_second)

    # Connection pool methods removed - each task now creates its own connection
    # This is simpler, more reliable, and avoids stale connection issues

    def get_client_info(self) -> Dict[str, Any]:
        """Get Kafka client information."""
        info = {
            'worker_id': self.worker_id,
            'client_type': self.client_type,
            'driver_name': self.driver_config.name if self.driver_config else 'unknown'
        }

        if self._driver:
            info.update(self._driver.get_client_info())

        return info


if __name__ == "__main__":
    import argparse
    import uvicorn
    import sys
    from pathlib import Path

    # Add project root to path
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))

    from benchmark.api.worker_api import run_worker_server

    parser = argparse.ArgumentParser(description='Kafka Worker')
    parser.add_argument('--port', type=int, default=8080, help='Port to run on')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--worker-id', default='digital-twin-worker-1', help='Worker ID')
    parser.add_argument('--driver-config', help='Driver config file')
    parser.add_argument('--use-multiprocessing', action='store_true', default=True,
                        help='Enable multiprocessing for high throughput (default: True)')
    parser.add_argument('--num-processes', type=int, default=4,
                        help='Number of sender processes (default: 4)')

    args = parser.parse_args()

    # Load driver config if provided
    driver_config = None
    if args.driver_config:
        from benchmark.core.config import ConfigLoader
        driver_config = ConfigLoader.load_driver(args.driver_config)
    else:
        # Default Kafka config
        from benchmark.core.config import DriverConfig
        driver_config = DriverConfig(
            name="Kafka",
            driverClass="benchmark.drivers.kafka.KafkaDriver",
            commonConfig={
                "bootstrap.servers": "localhost:9092"
            },
            consumerConfig={
                "auto.offset.reset": "earliest"  # CRITICAL: Must use earliest for benchmarks!
            }
        )

    # Create worker instance
    worker = KafkaWorker(
        worker_id=args.worker_id,
        driver_config=driver_config,
        use_multiprocessing=args.use_multiprocessing,
        num_processes=args.num_processes
    )

    print(f"🚀 Starting Digital Twin Kafka Worker")
    print(f"   Worker ID: {args.worker_id}")
    print(f"   Client: Confluent Kafka (Digital Twin Optimized)")
    print(f"   Multiprocessing: {'Enabled' if args.use_multiprocessing else 'Disabled'}")
    if args.use_multiprocessing:
        print(f"   Processes: {args.num_processes}")
    print(f"   Port: {args.port}")
    print(f"   Health URL: http://{args.host}:{args.port}/health")

    # Start the server
    asyncio.run(run_worker_server(worker, host=args.host, port=args.port))