"""Enhanced benchmark components adapted from my-benchmark project."""

import time
import asyncio
from typing import Dict, List, Any, Optional
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import numpy as np
from dataclasses import dataclass, field

from ..core.results import ThroughputStats, LatencyStats, ErrorStats, WorkerResult
from ..core.monitoring import SystemMonitor
from .logging import LoggerMixin


@dataclass
class EnhancedProducerConfig:
    """Enhanced producer configuration with advanced options."""
    topic: str
    num_messages: int
    message_size: int
    rate_limit: int = 0
    batch_size: int = 16384
    linger_ms: int = 10
    compression_type: str = "gzip"
    acks: str = "all"
    retries: int = 3
    enable_idempotence: bool = True
    max_in_flight_requests: int = 5


@dataclass
class EnhancedConsumerConfig:
    """Enhanced consumer configuration with advanced options."""
    topics: List[str]
    group_id: str
    test_duration_seconds: int
    max_poll_records: int = 500
    fetch_max_wait_ms: int = 500
    auto_commit_interval_ms: int = 1000
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 3000


class LatencyBenchmarkExtension(LoggerMixin):
    """Extended latency benchmarking capabilities from my-benchmark."""

    def __init__(self, bootstrap_servers: List[str]):
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.latencies = []

    async def test_end_to_end_latency(
        self,
        client_type: str,
        num_messages: int = 1000,
        message_size: int = 100,
        payload_data: Optional[bytes] = None
    ) -> Dict[str, float]:
        """Test end-to-end latency with enhanced measurements."""
        self.logger.info(f"Starting end-to-end latency test: {client_type}, {num_messages} messages")

        topic = f'latency-test-{int(time.time())}'
        self.latencies = []

        try:
            if client_type == 'kafka-python':
                return await self._test_kafka_python_latency(topic, num_messages, message_size, payload_data)
            elif client_type == 'confluent-kafka':
                return await self._test_confluent_kafka_latency(topic, num_messages, message_size, payload_data)
            elif client_type == 'aiokafka':
                return await self._test_aiokafka_latency(topic, num_messages, message_size, payload_data)
            else:
                raise ValueError(f"Unsupported client type: {client_type}")

        except Exception as e:
            self.logger.error(f"Latency test failed: {e}")
            return {'error': str(e)}

    async def _test_kafka_python_latency(
        self,
        topic: str,
        num_messages: int,
        message_size: int,
        payload_data: Optional[bytes]
    ) -> Dict[str, float]:
        """Test latency using kafka-python with enhanced measurement."""
        try:
            from kafka import KafkaProducer, KafkaConsumer
        except ImportError:
            return {'error': 'kafka-python not available'}

        # Create payload
        if payload_data:
            payload = payload_data[:message_size] if len(payload_data) >= message_size else payload_data * ((message_size // len(payload_data)) + 1)
            payload = payload[:message_size]
        else:
            payload = b'x' * message_size

        # Setup producer with optimized settings for latency
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks='all',
            retries=3,
            batch_size=1,  # Minimize batching for latency
            linger_ms=0,   # Send immediately
            compression_type=None,  # No compression for latency
            enable_idempotence=True
        )

        # Setup consumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=f'latency-test-group-{int(time.time())}',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            fetch_min_bytes=1,
            fetch_max_wait_ms=0,  # Return immediately
            consumer_timeout_ms=10000
        )

        latencies = []
        received_count = 0

        # Send messages and measure latency
        for i in range(min(num_messages, 1000)):  # Limit for latency test
            send_time = time.time_ns()

            message_data = {
                'id': i,
                'send_time': send_time,
                'payload': payload.decode('utf-8', errors='ignore')
            }

            # Send message
            import json
            producer.send(topic, value=json.dumps(message_data).encode())

            # Try to receive the message
            try:
                # Poll for messages
                message_batch = consumer.poll(timeout_ms=1000)
                for partition_msgs in message_batch.values():
                    for msg in partition_msgs:
                        receive_time = time.time_ns()
                        try:
                            msg_data = json.loads(msg.value.decode())
                            if 'send_time' in msg_data:
                                latency_ns = receive_time - msg_data['send_time']
                                latency_ms = latency_ns / 1_000_000
                                latencies.append(latency_ms)
                                received_count += 1
                        except json.JSONDecodeError:
                            continue

            except Exception as e:
                self.logger.warning(f"Failed to receive message: {e}")

            if received_count >= min(500, num_messages):  # Limit received messages
                break

        producer.close()
        consumer.close()

        return self._calculate_enhanced_latency_stats(latencies)

    async def _test_confluent_kafka_latency(
        self,
        topic: str,
        num_messages: int,
        message_size: int,
        payload_data: Optional[bytes]
    ) -> Dict[str, float]:
        """Test latency using confluent-kafka."""
        try:
            from confluent_kafka import Producer, Consumer
        except ImportError:
            return {'error': 'confluent-kafka not available'}

        # Create payload
        if payload_data:
            payload = payload_data[:message_size] if len(payload_data) >= message_size else payload_data * ((message_size // len(payload_data)) + 1)
            payload = payload[:message_size]
        else:
            payload = b'x' * message_size

        # Setup producer
        producer = Producer({
            'bootstrap.servers': ','.join(self.bootstrap_servers),
            'acks': 'all',
            'linger.ms': 0,
            'batch.size': 1
        })

        # Setup consumer
        consumer = Consumer({
            'bootstrap.servers': ','.join(self.bootstrap_servers),
            'group.id': f'latency-test-group-{int(time.time())}',
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe([topic])

        latencies = []
        received_count = 0

        # Send and receive messages
        for i in range(min(num_messages, 500)):
            send_time = time.time_ns()

            import json
            message_data = {
                'id': i,
                'send_time': send_time,
                'payload': payload.decode('utf-8', errors='ignore')
            }

            producer.produce(topic, value=json.dumps(message_data))
            producer.poll(0)

            # Try to receive
            start_poll = time.time()
            while time.time() - start_poll < 2:  # 2 second timeout
                msg = consumer.poll(timeout=0.1)
                if msg is None:
                    continue
                if msg.error():
                    continue

                try:
                    receive_time = time.time_ns()
                    msg_data = json.loads(msg.value().decode())
                    if 'send_time' in msg_data:
                        latency_ns = receive_time - msg_data['send_time']
                        latency_ms = latency_ns / 1_000_000
                        latencies.append(latency_ms)
                        received_count += 1
                        break
                except Exception:
                    continue

        producer.flush()
        consumer.close()

        return self._calculate_enhanced_latency_stats(latencies)

    async def _test_aiokafka_latency(
        self,
        topic: str,
        num_messages: int,
        message_size: int,
        payload_data: Optional[bytes]
    ) -> Dict[str, float]:
        """Test latency using aiokafka."""
        try:
            from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
        except ImportError:
            return {'error': 'aiokafka not available'}

        # Create payload
        if payload_data:
            payload = payload_data[:message_size] if len(payload_data) >= message_size else payload_data * ((message_size // len(payload_data)) + 1)
            payload = payload[:message_size]
        else:
            payload = b'x' * message_size

        # Setup producer
        producer = AIOKafkaProducer(
            bootstrap_servers=','.join(self.bootstrap_servers),
            acks='all',
            linger_ms=0,
            batch_size=1
        )
        await producer.start()

        # Setup consumer
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=','.join(self.bootstrap_servers),
            group_id=f'latency-test-group-{int(time.time())}',
            auto_offset_reset='earliest'
        )
        await consumer.start()

        latencies = []
        received_count = 0

        try:
            # Send and receive messages
            for i in range(min(num_messages, 500)):
                send_time = time.time_ns()

                import json
                message_data = {
                    'id': i,
                    'send_time': send_time,
                    'payload': payload.decode('utf-8', errors='ignore')
                }

                await producer.send_and_wait(topic, value=json.dumps(message_data).encode())

                # Try to receive
                try:
                    async for msg in consumer:
                        receive_time = time.time_ns()
                        try:
                            msg_data = json.loads(msg.value.decode())
                            if 'send_time' in msg_data:
                                latency_ns = receive_time - msg_data['send_time']
                                latency_ms = latency_ns / 1_000_000
                                latencies.append(latency_ms)
                                received_count += 1
                                break
                        except Exception:
                            continue

                        # Break after processing one message
                        break

                except asyncio.TimeoutError:
                    continue

        finally:
            await producer.stop()
            await consumer.stop()

        return self._calculate_enhanced_latency_stats(latencies)

    def _calculate_enhanced_latency_stats(self, latencies: List[float]) -> Dict[str, float]:
        """Calculate enhanced latency statistics."""
        if not latencies:
            return {'error': 'no_latency_data'}

        latencies.sort()
        n = len(latencies)

        if n == 0:
            return {'error': 'no_valid_latencies'}

        percentiles = [50, 90, 95, 99, 99.9, 99.99]
        stats = {
            'count': n,
            'avg_latency_ms': float(np.mean(latencies)),
            'median_latency_ms': float(np.median(latencies)),
            'min_latency_ms': float(min(latencies)),
            'max_latency_ms': float(max(latencies)),
            'std_dev_ms': float(np.std(latencies))
        }

        # Add percentiles
        for p in percentiles:
            if n > 1:
                stats[f'p{p}_latency_ms'] = float(np.percentile(latencies, p))
            else:
                stats[f'p{p}_latency_ms'] = float(latencies[0])

        return stats


class ComprehensiveBenchmarkSuite(LoggerMixin):
    """Comprehensive benchmark suite integrating enhanced capabilities."""

    def __init__(self, bootstrap_servers: List[str]):
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.results = {}

    async def run_comprehensive_test(
        self,
        clients: List[str] = None,
        test_config: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Run comprehensive test suite."""
        if clients is None:
            clients = self._get_available_clients()

        if test_config is None:
            test_config = self._get_default_test_config()

        self.logger.info(f"Starting comprehensive test for clients: {clients}")

        for client in clients:
            if not self._is_client_available(client):
                self.logger.warning(f"Client {client} not available, skipping")
                continue

            self.logger.info(f"Testing {client} client...")
            client_results = {}

            try:
                # 1. Enhanced latency test
                self.logger.info(f"{client} - Enhanced latency test")
                latency_bench = LatencyBenchmarkExtension(self.bootstrap_servers)
                client_results['latency'] = await latency_bench.test_end_to_end_latency(
                    client_type=client,
                    num_messages=test_config['latency']['num_messages'],
                    message_size=test_config['latency']['message_size']
                )

                self.results[client] = client_results

            except Exception as e:
                self.logger.error(f"Client {client} test failed: {e}")
                self.results[client] = {'error': str(e)}

        return self.results

    def _get_available_clients(self) -> List[str]:
        """Get available Kafka Python clients."""
        available = []

        try:
            import kafka
            available.append('kafka-python')
        except ImportError:
            pass

        try:
            import confluent_kafka
            available.append('confluent-kafka')
        except ImportError:
            pass

        try:
            import aiokafka
            available.append('aiokafka')
        except ImportError:
            pass

        return available

    def _is_client_available(self, client: str) -> bool:
        """Check if client is available."""
        try:
            if client == 'kafka-python':
                import kafka
            elif client == 'confluent-kafka':
                import confluent_kafka
            elif client == 'aiokafka':
                import aiokafka
            return True
        except ImportError:
            return False

    def _get_default_test_config(self) -> Dict[str, Any]:
        """Get default test configuration."""
        return {
            'latency': {
                'num_messages': 1000,
                'message_size': 100
            }
        }

    def generate_enhanced_report(self) -> str:
        """Generate enhanced performance report."""
        if not self.results:
            return "# No test results available\n"

        report = ["# Enhanced Python Kafka Benchmark Report\n"]
        report.append(f"Generated at: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        report.append(f"Tested clients: {', '.join(self.results.keys())}\n\n")

        # Enhanced comparison table
        report.append("## Enhanced Latency Comparison\n")
        report.append("| Client | Avg (ms) | P95 (ms) | P99 (ms) | P99.9 (ms) | P99.99 (ms) | Std Dev (ms) |")
        report.append("|--------|----------|----------|----------|------------|-------------|--------------|")

        for client, results in self.results.items():
            if 'error' in results:
                report.append(f"| {client} | ERROR | ERROR | ERROR | ERROR | ERROR | ERROR |")
                continue

            latency = results.get('latency', {})
            if 'error' in latency:
                report.append(f"| {client} | ERROR | ERROR | ERROR | ERROR | ERROR | ERROR |")
                continue

            avg = latency.get('avg_latency_ms', 0)
            p95 = latency.get('p95_latency_ms', 0)
            p99 = latency.get('p99_latency_ms', 0)
            p99_9 = latency.get('p99.9_latency_ms', 0)
            p99_99 = latency.get('p99.99_latency_ms', 0)
            std_dev = latency.get('std_dev_ms', 0)

            report.append(f"| {client} | {avg:.2f} | {p95:.2f} | {p99:.2f} | {p99_9:.2f} | {p99_99:.2f} | {std_dev:.2f} |")

        report.append("\n")

        # Detailed results for each client
        for client, results in self.results.items():
            report.append(f"## {client} Detailed Results\n")

            if 'error' in results:
                report.append(f"❌ Test failed: {results['error']}\n\n")
                continue

            if 'latency' in results and 'error' not in results['latency']:
                latency = results['latency']
                report.append("### Enhanced Latency Metrics\n")
                report.append(f"- Sample count: {latency.get('count', 0)}\n")
                report.append(f"- Average latency: {latency.get('avg_latency_ms', 0):.3f} ms\n")
                report.append(f"- Median latency: {latency.get('median_latency_ms', 0):.3f} ms\n")
                report.append(f"- Min latency: {latency.get('min_latency_ms', 0):.3f} ms\n")
                report.append(f"- Max latency: {latency.get('max_latency_ms', 0):.3f} ms\n")
                report.append(f"- Standard deviation: {latency.get('std_dev_ms', 0):.3f} ms\n")
                report.append(f"- P90 latency: {latency.get('p90_latency_ms', 0):.3f} ms\n")
                report.append(f"- P95 latency: {latency.get('p95_latency_ms', 0):.3f} ms\n")
                report.append(f"- P99 latency: {latency.get('p99_latency_ms', 0):.3f} ms\n")
                report.append(f"- P99.9 latency: {latency.get('p99.9_latency_ms', 0):.3f} ms\n")
                report.append(f"- P99.99 latency: {latency.get('p99.99_latency_ms', 0):.3f} ms\n\n")

        # Performance recommendations
        report.append("## Enhanced Performance Recommendations\n")
        report.append(self._generate_enhanced_recommendations())

        return "\n".join(report)

    def _generate_enhanced_recommendations(self) -> str:
        """Generate enhanced performance recommendations."""
        recommendations = []

        if not self.results:
            return "No test results available for recommendations.\n"

        # Analyze latency performance
        latency_results = {}
        for client, results in self.results.items():
            if 'latency' in results and 'avg_latency_ms' in results['latency']:
                latency_results[client] = {
                    'avg': results['latency']['avg_latency_ms'],
                    'p99': results['latency'].get('p99_latency_ms', 0),
                    'std_dev': results['latency'].get('std_dev_ms', 0)
                }

        if latency_results:
            # Find best performing client
            best_avg_client = min(latency_results, key=lambda x: latency_results[x]['avg'])
            best_p99_client = min(latency_results, key=lambda x: latency_results[x]['p99'])

            recommendations.append(f"**Best Average Latency**: {best_avg_client} ({latency_results[best_avg_client]['avg']:.2f}ms)")
            recommendations.append(f"**Best P99 Latency**: {best_p99_client} ({latency_results[best_p99_client]['p99']:.2f}ms)")

            # Consistency analysis
            most_consistent = min(latency_results, key=lambda x: latency_results[x]['std_dev'])
            recommendations.append(f"**Most Consistent Performance**: {most_consistent} (std dev: {latency_results[most_consistent]['std_dev']:.2f}ms)")

            # Performance warnings
            for client, metrics in latency_results.items():
                if metrics['p99'] > 100:  # P99 > 100ms
                    recommendations.append(f"⚠️  **{client}**: High P99 latency ({metrics['p99']:.2f}ms) - consider configuration tuning")

                if metrics['std_dev'] > 50:  # High variance
                    recommendations.append(f"⚠️  **{client}**: High latency variance ({metrics['std_dev']:.2f}ms) - performance inconsistent")

        # General recommendations
        recommendations.extend([
            "**For Low Latency**: Use minimal batching (batch.size=1, linger.ms=0)",
            "**For High Throughput**: Increase batching (batch.size=16384+, linger.ms=10+)",
            "**For Consistency**: Enable idempotence and use acks=all",
            "**For Production**: Monitor P99 latencies and implement proper error handling"
        ])

        return "\n".join(f"- {rec}" for rec in recommendations) + "\n"