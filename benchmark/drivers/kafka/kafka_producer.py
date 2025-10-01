"""Confluent Kafka producer implementation optimized for Digital Twin scenarios."""

import time
import asyncio
from typing import Dict, List, Any, Optional, Callable
from ..base import AbstractProducer, Message, ProducedMessage
from ...utils.logging import LoggerMixin
from ...utils.latency_recorder import LatencyRecorder


class KafkaProducer(AbstractProducer, LoggerMixin):
    """Confluent Kafka producer optimized for Digital Twin workloads."""

    def __init__(
        self,
        bootstrap_servers: str,
        config: Dict[str, Any],
        common_config: Optional[Dict[str, Any]] = None
    ):
        """Initialize Confluent Kafka producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            config: Producer configuration
            common_config: Common configuration
        """
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.config = config
        self.common_config = common_config or {}
        self._producer = None

        # Digital Twin optimizations
        self._delivery_reports = {}
        self._pending_messages = 0
        self._max_pending_messages = 10000  # Prevent memory issues

        # High-precision latency recording (like original OMB)
        self._latency_recorder = LatencyRecorder()
        self._message_send_times = {}  # Track send time for each message

        # Merge configurations with Digital Twin optimizations
        self._merged_config = self._merge_configs()

    def _merge_configs(self) -> Dict[str, Any]:
        """Merge common and producer-specific configurations with Digital Twin optimizations."""
        # Filter out admin-only configurations and Java-style configs that Producer doesn't support
        # librdkafka uses different config names than Java Kafka
        producer_incompatible_keys = {
            'default.api.timeout.ms',  # Admin API only
            'buffer.memory',            # Java config, use queue.buffering.max.kbytes instead
            'max.request.size',         # Java config, use message.max.bytes instead
        }

        merged = {k: v for k, v in self.common_config.items() if k not in producer_incompatible_keys}
        # Also filter from producer-specific config
        merged.update({k: v for k, v in self.config.items() if k not in producer_incompatible_keys})

        # Ensure bootstrap servers is set
        if "bootstrap.servers" not in merged:
            merged["bootstrap.servers"] = self.bootstrap_servers

        # Convert all config values to strings for confluent-kafka compatibility
        merged = {k: str(v) if not isinstance(v, str) else v for k, v in merged.items()}

        # Digital Twin scenario optimizations (Confluent Kafka compatible)
        digital_twin_defaults = {
            # High throughput settings for sensor data
            "batch.size": "65536",  # 64KB batches
            "linger.ms": "5",       # 5ms linger for better batching
            "compression.type": "lz4",  # Fast compression for real-time data

            # Reliability for critical Digital Twin data
            "acks": "all",
            "retries": "2147483647",  # Infinite retries
            "retry.backoff.ms": "100",
            "delivery.timeout.ms": "300000",  # 5 minutes

            # Memory and connection optimizations (Confluent Kafka specific)
            "queue.buffering.max.messages": "100000",  # Message buffer
            "queue.buffering.max.kbytes": "65536",     # 64MB buffer
            "message.max.bytes": "2097152",            # 2MB max message
            "socket.keepalive.enable": "true",

            # Idempotence for exactly-once semantics
            "enable.idempotence": "true",
        }

        # Apply defaults only if not explicitly set
        for key, value in digital_twin_defaults.items():
            if key not in merged:
                merged[key] = value

        return merged

    async def _initialize_producer(self):
        """Initialize the Confluent Kafka producer."""
        if self._producer is not None:
            return

        try:
            from confluent_kafka import Producer
        except ImportError:
            raise ImportError("confluent-kafka not available. Install with: pip install confluent-kafka")

        self._producer = Producer(self._merged_config)
        self.logger.info("Initialized Confluent Kafka producer for Digital Twin scenarios")

    def _delivery_callback(self, err, msg):
        """Delivery report callback for async message sending with latency tracking."""
        # Extract message ID - must match send_message() ID generation logic
        if msg.key():
            message_id = msg.key().decode()
        else:
            # For messages without keys, check for tracking header
            headers = msg.headers() or []
            track_id = None

            # DEBUG: Print headers to see what we're getting
            # self.logger.debug(f"Callback headers: {headers}")

            for header_key, header_value in headers:
                if header_key == '_track_id':
                    track_id = header_value.decode() if isinstance(header_value, bytes) else header_value
                    break

            if track_id:
                message_id = track_id
            else:
                # Fallback (should not happen if send_message works correctly)
                # Check if _track_id exists in send_times with any prefix
                for tracked_id in list(self._message_send_times.keys()):
                    if f"-{msg.offset()}" in tracked_id or tracked_id.startswith("msg-"):
                        # Try to match by checking pending send_times
                        # Since we don't have a way to match, just use the first available
                        message_id = tracked_id
                        break
                else:
                    message_id = f"msg-{msg.offset()}"

        # Record latency from send to acknowledgement
        if message_id in self._message_send_times:
            send_time = self._message_send_times[message_id]
            ack_time = time.time()
            latency_ms = (ack_time - send_time) * 1000

            if not err:
                # Only record latency for successful sends
                self._latency_recorder.record_latency(latency_ms)

            # Clean up send time tracking
            del self._message_send_times[message_id]

        if err:
            self.logger.error(f"Message delivery failed for {message_id}: {err}")
            self._delivery_reports[message_id] = {"success": False, "error": str(err)}
        else:
            self._delivery_reports[message_id] = {
                "success": True,
                "partition": msg.partition(),
                "offset": msg.offset()
            }

        self._pending_messages -= 1

    async def send_message(self, topic: str, message: Message) -> ProducedMessage:
        """Send a single message with Digital Twin optimizations.

        Note: confluent-kafka's produce() is non-blocking, no need for thread pool.
        """
        if self._producer is None:
            await self._initialize_producer()

        # Backpressure control for Digital Twin scenarios
        while self._pending_messages >= self._max_pending_messages:
            await asyncio.sleep(0.001)  # 1ms wait
            self._producer.poll(0)  # Process delivery reports

        send_time = time.time()

        # Generate unique message ID for latency tracking
        # IMPORTANT: This must match the ID generation in _delivery_callback
        # If message has no key, we need to use a unique ID that can be passed through
        # For messages without keys, we'll add a tracking header
        if message.key:
            message_id = message.key.decode()
        else:
            # Generate unique ID and add to headers for tracking
            message_id = f"msg-{int(send_time * 1000000)}-{id(message)}"
            if message.headers is None:
                message.headers = {}
            message.headers['_track_id'] = message_id.encode()

        # Track send time for latency calculation
        self._message_send_times[message_id] = send_time

        try:
            self._pending_messages += 1

            # Convert headers dict to list of tuples for confluent-kafka
            headers_list = None
            if message.headers:
                if isinstance(message.headers, dict):
                    headers_list = [(k, v) for k, v in message.headers.items()]
                else:
                    headers_list = message.headers

            # produce() is non-blocking - it queues the message and returns immediately
            # The callback will be invoked when delivery is confirmed
            self._producer.produce(
                topic=topic,
                key=message.key,
                value=message.value,
                headers=headers_list,
                timestamp=int(message.timestamp * 1000) if message.timestamp else None,
                callback=self._delivery_callback
            )

            # Poll for delivery reports (non-blocking, triggers callbacks)
            self._producer.poll(0)

        except Exception as e:
            self._pending_messages -= 1
            # Clean up send time on error
            if message_id in self._message_send_times:
                del self._message_send_times[message_id]
            raise

        return ProducedMessage(
            message=message,
            send_time=send_time,
            partition=None,  # Will be available in delivery report
            offset=None
        )

    async def send_batch(self, topic: str, messages: List[Message]) -> List[ProducedMessage]:
        """Send a batch of messages optimized for Digital Twin sensor data."""
        if self._producer is None:
            await self._initialize_producer()

        results = []
        batch_start_time = time.time()

        # Use semaphore to control concurrent sends for better Digital Twin performance
        semaphore = asyncio.Semaphore(100)

        async def send_with_semaphore(msg):
            async with semaphore:
                return await self.send_message(topic, msg)

        # Send messages concurrently but with controlled parallelism
        tasks = [send_with_semaphore(msg) for msg in messages]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results and handle exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"Batch message {i} failed: {result}")
                # Create error result
                error_result = ProducedMessage(
                    message=messages[i],
                    send_time=batch_start_time,
                    partition=None,
                    offset=None
                )
                processed_results.append(error_result)
            else:
                processed_results.append(result)

        batch_end_time = time.time()
        self.logger.debug(f"Sent batch of {len(messages)} messages in {batch_end_time - batch_start_time:.3f}s")

        return processed_results

    async def flush(self) -> None:
        """Flush pending messages with timeout suitable for Digital Twin scenarios."""
        if self._producer is None:
            return

        # Note: flush() is a blocking call, but it's necessary for ensuring message delivery
        # We run it in a separate thread to avoid blocking the event loop
        # This is one of the few justified uses of blocking I/O
        def do_flush():
            # 30 second timeout suitable for Digital Twin real-time requirements
            remaining = self._producer.flush(timeout=30)
            return remaining

        loop = asyncio.get_event_loop()
        remaining = await loop.run_in_executor(None, do_flush)

        if remaining > 0:
            self.logger.warning(f"Flushed producer, {remaining} messages still pending after timeout")
        else:
            self.logger.debug(f"Flushed producer successfully")

    async def close(self) -> None:
        """Close the producer and cleanup resources."""
        if self._producer is None:
            return

        try:
            # Flush before closing
            await self.flush()

            # Producer cleanup is automatic in confluent-kafka
            self._producer = None
            self._delivery_reports.clear()
            self._message_send_times.clear()

            self.logger.info("Closed Confluent Kafka producer")

        except Exception as e:
            self.logger.error(f"Failed to close producer: {e}")
            raise

    def get_metrics(self) -> Dict[str, Any]:
        """Get producer metrics for Digital Twin monitoring."""
        if self._producer is None:
            return {}

        return {
            "pending_messages": self._pending_messages,
            "delivery_reports": len(self._delivery_reports),
            "client_type": "confluent-kafka",
            "optimized_for": "digital_twin_scenarios",
            "latency_samples": self._latency_recorder._count
        }

    def get_latency_snapshot(self):
        """Get latency statistics snapshot with accurate percentiles.

        Returns:
            LatencySnapshot with precise percentile calculations using HdrHistogram
        """
        return self._latency_recorder.get_snapshot()

    def reset_latency_stats(self) -> None:
        """Reset latency statistics."""
        self._latency_recorder.reset()

    async def wait_for_delivery(self, timeout_seconds: float = 60.0) -> Dict[str, Any]:
        """Wait for all pending message deliveries to complete.

        This ensures all callbacks have been executed and latency stats are complete.
        Critical for accurate benchmarking results.

        Args:
            timeout_seconds: Maximum time to wait for delivery confirmations

        Returns:
            Dictionary with delivery status
        """
        if self._producer is None:
            return {"pending_messages": 0, "elapsed_time": 0, "timed_out": False}

        start_time = time.time()
        initial_pending = self._pending_messages

        # Poll for delivery reports until all messages are delivered or timeout
        while self._pending_messages > 0:
            elapsed = time.time() - start_time
            if elapsed >= timeout_seconds:
                break

            # Non-blocking poll to trigger callbacks
            self._producer.poll(0.1)  # 100ms poll
            await asyncio.sleep(0.01)  # Small sleep to avoid busy-waiting

        elapsed_time = time.time() - start_time
        timed_out = self._pending_messages > 0

        if timed_out:
            self.logger.warning(
                f"Delivery wait timed out after {elapsed_time:.1f}s: "
                f"{self._pending_messages}/{initial_pending} messages still pending"
            )

        return {
            "pending_messages": self._pending_messages,
            "elapsed_time": elapsed_time,
            "timed_out": timed_out,
            "initial_pending": initial_pending,
            "completed": initial_pending - self._pending_messages
        }