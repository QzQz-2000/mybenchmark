"""Parallel message sender using multiprocessing for high throughput.

This module provides a multiprocessing-based sender that bypasses Python's GIL
to achieve higher throughput, similar to original OMB's multi-threaded approach.
"""

import time
import multiprocessing as mp
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import asyncio


@dataclass
class SendTask:
    """Task for a single sender process."""
    process_id: int
    topic: str
    num_messages: int
    message_size: int
    payload: bytes
    rate_per_second: float  # Rate for this process
    bootstrap_servers: str
    producer_config: Dict[str, Any]
    start_index: int  # 消息起始编号


class ParallelSender:
    """Parallel message sender using multiple processes.

    This achieves higher throughput by:
    1. Bypassing Python's GIL with multiprocessing
    2. Each process has its own Kafka producer
    3. Rate limiting is distributed across processes
    """

    def __init__(self, num_processes: int = 4):
        """Initialize parallel sender.

        Args:
            num_processes: Number of sender processes (default: 4)
        """
        self.num_processes = num_processes
        self._results_queue = None
        self._processes = []

    async def send_parallel(
        self,
        topic: str,
        num_messages: int,
        message_size: int,
        payload: bytes,
        rate_per_second: float,
        bootstrap_servers: str,
        producer_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Send messages in parallel across multiple processes.

        Args:
            topic: Kafka topic
            num_messages: Total number of messages
            message_size: Size of each message
            payload: Message payload
            rate_per_second: Total target rate (divided across processes)
            bootstrap_servers: Kafka bootstrap servers
            producer_config: Producer configuration

        Returns:
            Aggregated statistics from all processes
        """
        # Divide messages and rate across processes
        messages_per_process = num_messages // self.num_processes
        rate_per_process = rate_per_second / self.num_processes

        # Create result queue for collecting stats
        self._results_queue = mp.Queue()

        # Create and start sender processes
        self._processes = []
        start_time = time.time()

        for i in range(self.num_processes):
            # Last process handles remaining messages
            process_messages = messages_per_process
            if i == self.num_processes - 1:
                process_messages = num_messages - (messages_per_process * (self.num_processes - 1))

            task = SendTask(
                process_id=i,
                topic=topic,
                num_messages=process_messages,
                message_size=message_size,
                payload=payload,
                rate_per_second=rate_per_process,
                bootstrap_servers=bootstrap_servers,
                producer_config=producer_config,
                start_index=i * messages_per_process
            )

            process = mp.Process(
                target=_sender_process_worker,
                args=(task, self._results_queue)
            )
            process.start()
            self._processes.append(process)

        # Wait for all processes to complete
        for process in self._processes:
            process.join()

        end_time = time.time()

        # Collect results from all processes
        results = []
        while not self._results_queue.empty():
            result = self._results_queue.get()
            results.append(result)
            # Log errors if any
            if 'error' in result:
                print(f"Process {result['process_id']} error: {result['error']}")

        # Aggregate statistics
        aggregated = self._aggregate_results(results, start_time, end_time)

        return aggregated

    def _aggregate_results(
        self,
        results: List[Dict[str, Any]],
        start_time: float,
        end_time: float
    ) -> Dict[str, Any]:
        """Aggregate results from all processes."""
        total_messages = sum(r['messages_sent'] for r in results)
        total_bytes = sum(r['total_bytes'] for r in results)
        total_errors = sum(r['errors'] for r in results)

        # Merge latency data from all processes
        all_latencies = []
        for r in results:
            all_latencies.extend(r.get('latencies', []))

        # Calculate latency statistics using all collected samples
        latency_stats = None
        if all_latencies:
            all_latencies.sort()
            count = len(all_latencies)
            latency_stats = {
                'count': count,
                'min_ms': all_latencies[0],
                'max_ms': all_latencies[-1],
                'mean_ms': sum(all_latencies) / count,
                'median_ms': all_latencies[count // 2],
                'p50_ms': all_latencies[int(count * 0.50)],
                'p95_ms': all_latencies[int(count * 0.95)] if count > 20 else all_latencies[-1],
                'p99_ms': all_latencies[int(count * 0.99)] if count > 100 else all_latencies[-1],
                'p99_9_ms': all_latencies[int(count * 0.999)] if count > 1000 else all_latencies[-1]
            }

        # Calculate statistics
        duration = end_time - start_time

        return {
            'total_messages': total_messages,
            'total_bytes': total_bytes,
            'total_errors': total_errors,
            'duration_seconds': duration,
            'messages_per_second': total_messages / duration if duration > 0 else 0,
            'latency_stats': latency_stats,
            'all_latencies': all_latencies[:10000],  # Keep first 10K for further analysis
            'process_count': len(results),
            'process_results': results
        }

    def cleanup(self):
        """Cleanup resources with proper shutdown sequence."""
        if not self._processes:
            return

        # First, try graceful shutdown
        for process in self._processes:
            if process.is_alive():
                process.join(timeout=2)  # Give 2 seconds for graceful exit

        # Then terminate if still alive
        for process in self._processes:
            if process.is_alive():
                print(f"Process {process.pid} still alive, terminating...", flush=True)
                process.terminate()
                process.join(timeout=3)

        # Finally, force kill if necessary
        for process in self._processes:
            if process.is_alive():
                print(f"Process {process.pid} did not terminate, killing...", flush=True)
                process.kill()
                process.join(timeout=1)

        self._processes.clear()


def _sender_process_worker(task: SendTask, results_queue: mp.Queue):
    """Worker function for sender process.

    This runs in a separate process and has its own Kafka producer.
    """
    try:
        # Import here to avoid issues with multiprocessing
        from confluent_kafka import Producer
        import time

        # Create Kafka producer for this process
        producer_config = task.producer_config.copy()
        producer_config['bootstrap.servers'] = task.bootstrap_servers

        producer = Producer(producer_config)

        # Statistics tracking
        messages_sent = 0
        errors = 0
        latencies = []
        send_times = {}

        def delivery_callback(err, msg):
            nonlocal messages_sent, errors
            message_key = msg.key().decode() if msg.key() else str(msg.offset())

            if err:
                errors += 1
            else:
                messages_sent += 1

                # Track latency if we have send time
                if message_key in send_times:
                    latency_ms = (time.time() - send_times[message_key]) * 1000
                    latencies.append(latency_ms)
                    del send_times[message_key]

        # Rate limiting setup with improved precision
        if task.rate_per_second > 0:
            interval_ns = int(1_000_000_000 / task.rate_per_second)  # nanoseconds
        else:
            interval_ns = 0

        # Send messages with high-precision rate limiting
        start_time_ns = time.time_ns()
        start_time = time.time()
        print(f"[Process {task.process_id}] Starting to send {task.num_messages} messages to topic '{task.topic}'", flush=True)

        for i in range(task.num_messages):
            # Apply rate limiting with nanosecond precision
            if interval_ns > 0:
                target_time_ns = start_time_ns + i * interval_ns
                current_time_ns = time.time_ns()
                wait_ns = target_time_ns - current_time_ns

                if wait_ns > 0:
                    # Sleep with microsecond precision
                    wait_seconds = wait_ns / 1_000_000_000
                    time.sleep(wait_seconds)

            # Create message
            message_key = f"msg-p{task.process_id}-{task.start_index + i}"
            send_time = time.time()
            send_times[message_key] = send_time

            # Add send timestamp to payload for E2E latency
            send_timestamp_ms = int(send_time * 1000)
            headers = [
                ('send_timestamp', str(send_timestamp_ms).encode()),
                ('process_id', str(task.process_id).encode())
            ]

            try:
                producer.produce(
                    topic=task.topic,
                    key=message_key.encode(),
                    value=task.payload,
                    headers=headers,
                    callback=delivery_callback
                )

                # OPTIMIZATION: Adaptive polling frequency
                # Poll more frequently when buffer is filling up
                # This prevents BufferError while maintaining high throughput
                if i % 50 == 0:  # Poll every 50 messages (was 100)
                    producer.poll(0)
                elif i % 10 == 0:  # Quick poll every 10 messages
                    producer.poll(0)

            except BufferError:
                # Producer queue full, wait and retry
                producer.poll(0.1)
                # Retry once
                try:
                    producer.produce(
                        topic=task.topic,
                        key=message_key.encode(),
                        value=task.payload,
                        headers=headers,
                        callback=delivery_callback
                    )
                except Exception as retry_error:
                    errors += 1
                    print(f"[Process {task.process_id}] BufferError retry failed for message {i}: {retry_error}", flush=True)
            except Exception as e:
                errors += 1
                # Log first few errors for debugging
                if errors <= 10:
                    print(f"[Process {task.process_id}] Send error for message {i}: {type(e).__name__}: {e}", flush=True)

        # Flush remaining messages with better error handling
        print(f"[Process {task.process_id}] Flushing producer...", flush=True)
        remaining_messages = producer.flush(timeout=30)
        if remaining_messages > 0:
            print(f"[Process {task.process_id}] WARNING: {remaining_messages} messages still in queue after flush", flush=True)
            errors += remaining_messages

        end_time = time.time()
        duration = end_time - start_time
        actual_rate = messages_sent / duration if duration > 0 else 0

        # Collect results with ALL latencies for accurate statistics
        result = {
            'process_id': task.process_id,
            'messages_sent': messages_sent,
            'total_bytes': messages_sent * task.message_size,
            'errors': errors,
            'latencies': latencies,  # Keep ALL latencies for accurate aggregation
            'duration': duration,
            'actual_rate': actual_rate,
            'latency_count': len(latencies)
        }

        print(f"[Process {task.process_id}] Sent {messages_sent}/{task.num_messages} messages in {duration:.2f}s "
              f"(rate: {actual_rate:.1f} msg/s, target: {task.rate_per_second:.1f} msg/s, "
              f"latencies collected: {len(latencies)})", flush=True)

        results_queue.put(result)

    except Exception as e:
        # Report error with traceback
        import traceback
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"ERROR in process {task.process_id}: {error_msg}", flush=True)
        results_queue.put({
            'process_id': task.process_id,
            'messages_sent': 0,
            'total_bytes': 0,
            'errors': 1,
            'latencies': [],
            'error': error_msg
        })
    finally:
        # Ensure producer is properly closed even on error
        try:
            if 'producer' in locals():
                # Final flush with short timeout
                remaining = producer.flush(timeout=5)
                if remaining > 0:
                    print(f"[Process {task.process_id}] {remaining} messages not delivered during cleanup", flush=True)
                # Producer will be automatically cleaned up by confluent-kafka
        except Exception as cleanup_error:
            print(f"[Process {task.process_id}] Cleanup error: {cleanup_error}", flush=True)


async def test_parallel_sender():
    """Test function for parallel sender."""
    print("Testing Parallel Sender...")

    sender = ParallelSender(num_processes=4)

    # Test configuration
    payload = b"x" * 1024  # 1KB message

    result = await sender.send_parallel(
        topic="test-topic",
        num_messages=10000,
        message_size=1024,
        payload=payload,
        rate_per_second=4000,  # 4000 msg/s across 4 processes
        bootstrap_servers="localhost:9092",
        producer_config={
            'acks': 'all',
            'linger.ms': '1',
            'batch.size': '16384'
        }
    )

    print(f"\nResults:")
    print(f"  Messages sent: {result['total_messages']}")
    print(f"  Duration: {result['duration_seconds']:.2f}s")
    print(f"  Throughput: {result['messages_per_second']:.2f} msg/s")
    print(f"  Errors: {result['total_errors']}")
    print(f"  Processes: {result['process_count']}")

    sender.cleanup()


if __name__ == "__main__":
    asyncio.run(test_parallel_sender())