# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import logging
import random
from typing import List
from concurrent.futures import ThreadPoolExecutor
from .test_result import TestResult
from .workload import Workload
from .rate_controller import RateController

logger = logging.getLogger(__name__)


class WorkloadGenerator:
    """WorkloadGenerator implements AutoCloseable."""

    def __init__(self, driver_name: str, workload: Workload, worker):
        """
        Initialize workload generator.

        :param driver_name: Name of the driver
        :param workload: Workload configuration
        :param worker: Worker instance
        """
        self.driver_name = driver_name
        self.workload = workload
        self.worker = worker

        self.executor = ThreadPoolExecutor(thread_name_prefix="messaging-benchmark")

        self.run_completed = False
        self.need_to_wait_for_backlog_draining = False

        self.target_publish_rate = 0.0

        if workload.consumer_backlog_size_gb > 0 and workload.producer_rate == 0:
            raise ValueError("Cannot probe producer sustainable rate when building backlog")

    def run(self) -> TestResult:
        """Run the workload and return test results."""
        from benchmark.utils.timer import Timer
        from benchmark.worker.commands.topics_info import TopicsInfo

        timer = Timer()
        topics = self.worker.create_topics(
            TopicsInfo(self.workload.topics, self.workload.partitions_per_topic)
        )
        logger.info(f"Created {len(topics)} topics in {timer.elapsed_millis()} ms")

        self._create_consumers(topics)
        self._create_producers(topics)

        self._ensure_topics_are_ready()

        if self.workload.producer_rate > 0:
            self.target_publish_rate = self.workload.producer_rate
        else:
            # Producer rate is 0 and we need to discover the sustainable rate
            self.target_publish_rate = 10000

            self.executor.submit(self._find_maximum_sustainable_rate, self.target_publish_rate)

        from benchmark.utils.payload.file_payload_reader import FilePayloadReader
        from benchmark.worker.commands.producer_work_assignment import ProducerWorkAssignment

        payload_reader = FilePayloadReader(self.workload.message_size)

        producer_work_assignment = ProducerWorkAssignment()
        producer_work_assignment.key_distributor_type = self.workload.key_distributor
        producer_work_assignment.publish_rate = self.target_publish_rate
        producer_work_assignment.payload_data = []

        if self.workload.use_randomized_payloads:
            # create messages that are part random and part zeros
            # better for testing effects of compression
            r = random.Random()
            random_bytes = int(self.workload.message_size * self.workload.random_bytes_ratio)
            zeroed_bytes = self.workload.message_size - random_bytes

            for i in range(self.workload.randomized_payload_pool_size):
                rand_array = r.randbytes(random_bytes)
                zeroed_array = bytes(zeroed_bytes)
                combined = rand_array + zeroed_array
                producer_work_assignment.payload_data.append(combined)
        else:
            # Only load payload file if one is specified
            if self.workload.payload_file is not None:
                producer_work_assignment.payload_data.append(
                    payload_reader.load(self.workload.payload_file)
                )
            else:
                # Generate simple payload of the specified size
                producer_work_assignment.payload_data.append(
                    bytes(self.workload.message_size)
                )

        self.worker.start_load(producer_work_assignment)

        if self.workload.warmup_duration_minutes > 0:
            logger.info(f"----- Starting warm-up traffic ({self.workload.warmup_duration_minutes}m) ------")
            self._print_and_collect_stats(self.workload.warmup_duration_minutes * 60)

        if self.workload.consumer_backlog_size_gb > 0:
            self.executor.submit(self._build_and_drain_backlog, self.workload.test_duration_minutes)

        self.worker.reset_stats()
        logger.info(f"----- Starting benchmark traffic ({self.workload.test_duration_minutes}m)------")

        result = self._print_and_collect_stats(self.workload.test_duration_minutes * 60)
        self.run_completed = True

        self.worker.stop_all()
        return result

    def _ensure_topics_are_ready(self):
        """Ensure topics are ready by probing producers and waiting for consumers."""
        logger.info("Waiting for consumers to be ready")
        # This is work around the fact that there's no way to have a consumer ready in Kafka without
        # first publishing
        # some message on the topic, which will then trigger the partitions assignment to the consumers

        expected_messages = self.workload.topics * self.workload.subscriptions_per_topic

        # In this case we just publish 1 message and then wait for consumers to receive the data
        self.worker.probe_producers()

        start = time.time()
        end = start + 60

        while time.time() < end:
            stats = self.worker.get_counters_stats()

            logger.info(
                f"Waiting for topics to be ready -- Sent: {stats.messages_sent}, "
                f"Received: {stats.messages_received}"
            )

            if stats.messages_received < expected_messages:
                try:
                    time.sleep(2)
                except KeyboardInterrupt:
                    raise RuntimeError("Interrupted")
            else:
                break

        if time.time() >= end:
            raise RuntimeError("Timed out waiting for consumers to be ready")
        else:
            logger.info("All consumers are ready")

    def _find_maximum_sustainable_rate(self, current_rate: float):
        """
        Adjust the publish rate to a level that is sustainable, meaning that we can consume all the
        messages that are being produced.

        :param current_rate: Current rate
        """
        stats = self.worker.get_counters_stats()

        control_period_millis = 3000
        last_control_timestamp = time.perf_counter_ns()

        rate_controller = RateController()

        while not self.run_completed:
            # Check every few seconds and adjust the rate
            try:
                time.sleep(control_period_millis / 1000)
            except KeyboardInterrupt:
                return

            # Consider multiple copies when using multiple subscriptions
            stats = self.worker.get_counters_stats()
            current_time = time.perf_counter_ns()
            period_nanos = current_time - last_control_timestamp

            last_control_timestamp = current_time

            current_rate = rate_controller.next_rate(
                current_rate, period_nanos, stats.messages_sent, stats.messages_received
            )
            self.worker.adjust_publish_rate(current_rate)

    def close(self):
        """Close and cleanup resources."""
        self.worker.stop_all()
        self.executor.shutdown(wait=False)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _create_consumers(self, topics: List[str]):
        """Create consumers for topics."""
        from benchmark.worker.commands.consumer_assignment import ConsumerAssignment
        from benchmark.worker.commands.topic_subscription import TopicSubscription
        from benchmark.utils.random_generator import RandomGenerator
        from benchmark.utils.timer import Timer

        consumer_assignment = ConsumerAssignment()

        for topic in topics:
            for i in range(self.workload.subscriptions_per_topic):
                subscription_name = f"sub-{i:03d}-{RandomGenerator.get_random_string()}"
                for j in range(self.workload.consumer_per_subscription):
                    consumer_assignment.topics_subscriptions.append(
                        TopicSubscription(topic, subscription_name)
                    )

        random.shuffle(consumer_assignment.topics_subscriptions)

        timer = Timer()

        self.worker.create_consumers(consumer_assignment)
        logger.info(
            f"Created {len(consumer_assignment.topics_subscriptions)} consumers in "
            f"{timer.elapsed_millis()} ms"
        )

    def _create_producers(self, topics: List[str]):
        """Create producers for topics."""
        from benchmark.utils.timer import Timer

        full_list_of_topics = []

        # Add the topic multiple times, one for each producer
        for i in range(self.workload.producers_per_topic):
            full_list_of_topics.extend(topics)

        random.shuffle(full_list_of_topics)

        timer = Timer()

        self.worker.create_producers(full_list_of_topics)
        logger.info(f"Created {len(full_list_of_topics)} producers in {timer.elapsed_millis()} ms")

    def _build_and_drain_backlog(self, test_duration_minutes: int):
        """Build and drain message backlog."""
        from benchmark.utils.timer import Timer

        timer = Timer()
        logger.info("Stopping all consumers to build backlog")
        self.worker.pause_consumers()

        self.need_to_wait_for_backlog_draining = True

        requested_backlog_size = self.workload.consumer_backlog_size_gb * 1024 * 1024 * 1024

        while True:
            stats = self.worker.get_counters_stats()
            current_backlog_size = (
                self.workload.subscriptions_per_topic * stats.messages_sent - stats.messages_received
            ) * self.workload.message_size

            if current_backlog_size >= requested_backlog_size:
                break

            try:
                time.sleep(1)
            except KeyboardInterrupt:
                raise RuntimeError("Interrupted")

        logger.info(f"--- Completed backlog build in {timer.elapsed_seconds()} s ---")
        timer = Timer()
        logger.info("--- Start draining backlog ---")

        self.worker.resume_consumers()

        backlog_message_capacity = requested_backlog_size // self.workload.message_size
        backlog_empty_level = int((1.0 - self.workload.backlog_drain_ratio) * backlog_message_capacity)
        min_backlog = max(1000, backlog_empty_level)

        while True:
            stats = self.worker.get_counters_stats()
            current_backlog = (
                self.workload.subscriptions_per_topic * stats.messages_sent - stats.messages_received
            )

            if current_backlog <= min_backlog:
                logger.info(f"--- Completed backlog draining in {timer.elapsed_seconds()} s ---")

                try:
                    time.sleep(test_duration_minutes * 60)
                except KeyboardInterrupt:
                    raise RuntimeError("Interrupted")

                self.need_to_wait_for_backlog_draining = False
                return

            try:
                time.sleep(0.1)
            except KeyboardInterrupt:
                raise RuntimeError("Interrupted")

    def _print_and_collect_stats(self, test_duration_seconds: int) -> TestResult:
        """Print and collect statistics during the test."""
        from benchmark.utils.padding_decimal_format import PaddingDecimalFormat

        start_time = time.perf_counter_ns()

        # Print report stats
        old_time = time.perf_counter_ns()

        test_end_time = start_time + test_duration_seconds * 1_000_000_000 if test_duration_seconds > 0 else float('inf')

        result = TestResult()
        result.workload = self.workload.name
        result.driver = self.driver_name
        result.topics = self.workload.topics
        result.partitions = self.workload.partitions_per_topic
        result.message_size = self.workload.message_size
        result.producers_per_topic = self.workload.producers_per_topic
        result.consumers_per_topic = self.workload.consumer_per_subscription

        rate_format = PaddingDecimalFormat("0.0", 7)
        throughput_format = PaddingDecimalFormat("0.0", 4)
        dec = PaddingDecimalFormat("0.0", 4)

        while True:
            try:
                time.sleep(10)
            except KeyboardInterrupt:
                break

            stats = self.worker.get_period_stats()

            now = time.perf_counter_ns()
            elapsed = (now - old_time) / 1e9

            publish_rate = stats.messages_sent / elapsed
            publish_throughput = stats.bytes_sent / elapsed / 1024 / 1024
            error_rate = stats.message_send_errors / elapsed

            consume_rate = stats.messages_received / elapsed
            consume_throughput = stats.bytes_received / elapsed / 1024 / 1024

            current_backlog = max(
                0,
                self.workload.subscriptions_per_topic * stats.total_messages_sent
                - stats.total_messages_received
            )

            logger.info(
                f"Pub rate {rate_format.format(publish_rate)} msg/s / "
                f"{throughput_format.format(publish_throughput)} MB/s | "
                f"Pub err {rate_format.format(error_rate)} err/s | "
                f"Cons rate {rate_format.format(consume_rate)} msg/s / "
                f"{throughput_format.format(consume_throughput)} MB/s | "
                f"Backlog: {dec.format(current_backlog / 1000.0)} K | "
                f"Pub Latency (ms) avg: {dec.format(self._micros_to_millis(stats.publish_latency.get_mean_value()))} - "
                f"50%: {dec.format(self._micros_to_millis(stats.publish_latency.get_value_at_percentile(50)))} - "
                f"99%: {dec.format(self._micros_to_millis(stats.publish_latency.get_value_at_percentile(99)))} - "
                f"99.9%: {dec.format(self._micros_to_millis(stats.publish_latency.get_value_at_percentile(99.9)))} - "
                f"Max: {throughput_format.format(self._micros_to_millis(stats.publish_latency.get_max_value()))} | "
                f"Pub Delay Latency (us) avg: {dec.format(stats.publish_delay_latency.get_mean_value())} - "
                f"50%: {dec.format(stats.publish_delay_latency.get_value_at_percentile(50))} - "
                f"99%: {dec.format(stats.publish_delay_latency.get_value_at_percentile(99))} - "
                f"99.9%: {dec.format(stats.publish_delay_latency.get_value_at_percentile(99.9))} - "
                f"Max: {throughput_format.format(stats.publish_delay_latency.get_max_value())}"
            )

            result.publish_rate.append(publish_rate)
            result.publish_error_rate.append(error_rate)
            result.consume_rate.append(consume_rate)
            result.backlog.append(current_backlog)
            result.publish_latency_avg.append(self._micros_to_millis(stats.publish_latency.get_mean_value()))
            result.publish_latency_50pct.append(self._micros_to_millis(stats.publish_latency.get_value_at_percentile(50)))
            result.publish_latency_75pct.append(self._micros_to_millis(stats.publish_latency.get_value_at_percentile(75)))
            result.publish_latency_95pct.append(self._micros_to_millis(stats.publish_latency.get_value_at_percentile(95)))
            result.publish_latency_99pct.append(self._micros_to_millis(stats.publish_latency.get_value_at_percentile(99)))
            result.publish_latency_999pct.append(self._micros_to_millis(stats.publish_latency.get_value_at_percentile(99.9)))
            result.publish_latency_9999pct.append(self._micros_to_millis(stats.publish_latency.get_value_at_percentile(99.99)))
            result.publish_latency_max.append(self._micros_to_millis(stats.publish_latency.get_max_value()))

            result.publish_delay_latency_avg.append(stats.publish_delay_latency.get_mean_value())
            result.publish_delay_latency_50pct.append(int(stats.publish_delay_latency.get_value_at_percentile(50)))
            result.publish_delay_latency_75pct.append(int(stats.publish_delay_latency.get_value_at_percentile(75)))
            result.publish_delay_latency_95pct.append(int(stats.publish_delay_latency.get_value_at_percentile(95)))
            result.publish_delay_latency_99pct.append(int(stats.publish_delay_latency.get_value_at_percentile(99)))
            result.publish_delay_latency_999pct.append(int(stats.publish_delay_latency.get_value_at_percentile(99.9)))
            result.publish_delay_latency_9999pct.append(int(stats.publish_delay_latency.get_value_at_percentile(99.99)))
            result.publish_delay_latency_max.append(int(stats.publish_delay_latency.get_max_value()))

            result.end_to_end_latency_avg.append(self._micros_to_millis(stats.end_to_end_latency.get_mean_value()))
            result.end_to_end_latency_50pct.append(self._micros_to_millis(stats.end_to_end_latency.get_value_at_percentile(50)))
            result.end_to_end_latency_75pct.append(self._micros_to_millis(stats.end_to_end_latency.get_value_at_percentile(75)))
            result.end_to_end_latency_95pct.append(self._micros_to_millis(stats.end_to_end_latency.get_value_at_percentile(95)))
            result.end_to_end_latency_99pct.append(self._micros_to_millis(stats.end_to_end_latency.get_value_at_percentile(99)))
            result.end_to_end_latency_999pct.append(self._micros_to_millis(stats.end_to_end_latency.get_value_at_percentile(99.9)))
            result.end_to_end_latency_9999pct.append(self._micros_to_millis(stats.end_to_end_latency.get_value_at_percentile(99.99)))
            result.end_to_end_latency_max.append(self._micros_to_millis(stats.end_to_end_latency.get_max_value()))

            if now >= test_end_time and not self.need_to_wait_for_backlog_draining:
                agg = self.worker.get_cumulative_latencies()
                logger.info(
                    f"----- Aggregated Pub Latency (ms) avg: {dec.format(agg.publish_latency.get_mean_value() / 1000.0)} - "
                    f"50%: {dec.format(agg.publish_latency.get_value_at_percentile(50) / 1000.0)} - "
                    f"95%: {dec.format(agg.publish_latency.get_value_at_percentile(95) / 1000.0)} - "
                    f"99%: {dec.format(agg.publish_latency.get_value_at_percentile(99) / 1000.0)} - "
                    f"99.9%: {dec.format(agg.publish_latency.get_value_at_percentile(99.9) / 1000.0)} - "
                    f"99.99%: {dec.format(agg.publish_latency.get_value_at_percentile(99.99) / 1000.0)} - "
                    f"Max: {throughput_format.format(agg.publish_latency.get_max_value() / 1000.0)} | "
                    f"Pub Delay (us) avg: {dec.format(agg.publish_delay_latency.get_mean_value())} - "
                    f"50%: {dec.format(agg.publish_delay_latency.get_value_at_percentile(50))} - "
                    f"95%: {dec.format(agg.publish_delay_latency.get_value_at_percentile(95))} - "
                    f"99%: {dec.format(agg.publish_delay_latency.get_value_at_percentile(99))} - "
                    f"99.9%: {dec.format(agg.publish_delay_latency.get_value_at_percentile(99.9))} - "
                    f"99.99%: {dec.format(agg.publish_delay_latency.get_value_at_percentile(99.99))} - "
                    f"Max: {throughput_format.format(agg.publish_delay_latency.get_max_value())}"
                )

                result.aggregated_publish_latency_avg = agg.publish_latency.get_mean_value() / 1000.0
                result.aggregated_publish_latency_50pct = agg.publish_latency.get_value_at_percentile(50) / 1000.0
                result.aggregated_publish_latency_75pct = agg.publish_latency.get_value_at_percentile(75) / 1000.0
                result.aggregated_publish_latency_95pct = agg.publish_latency.get_value_at_percentile(95) / 1000.0
                result.aggregated_publish_latency_99pct = agg.publish_latency.get_value_at_percentile(99) / 1000.0
                result.aggregated_publish_latency_999pct = agg.publish_latency.get_value_at_percentile(99.9) / 1000.0
                result.aggregated_publish_latency_9999pct = agg.publish_latency.get_value_at_percentile(99.99) / 1000.0
                result.aggregated_publish_latency_max = agg.publish_latency.get_max_value() / 1000.0

                result.aggregated_publish_delay_latency_avg = agg.publish_delay_latency.get_mean_value()
                result.aggregated_publish_delay_latency_50pct = int(agg.publish_delay_latency.get_value_at_percentile(50))
                result.aggregated_publish_delay_latency_75pct = int(agg.publish_delay_latency.get_value_at_percentile(75))
                result.aggregated_publish_delay_latency_95pct = int(agg.publish_delay_latency.get_value_at_percentile(95))
                result.aggregated_publish_delay_latency_99pct = int(agg.publish_delay_latency.get_value_at_percentile(99))
                result.aggregated_publish_delay_latency_999pct = int(agg.publish_delay_latency.get_value_at_percentile(99.9))
                result.aggregated_publish_delay_latency_9999pct = int(agg.publish_delay_latency.get_value_at_percentile(99.99))
                result.aggregated_publish_delay_latency_max = int(agg.publish_delay_latency.get_max_value())

                result.aggregated_end_to_end_latency_avg = agg.end_to_end_latency.get_mean_value() / 1000.0
                result.aggregated_end_to_end_latency_50pct = agg.end_to_end_latency.get_value_at_percentile(50) / 1000.0
                result.aggregated_end_to_end_latency_75pct = agg.end_to_end_latency.get_value_at_percentile(75) / 1000.0
                result.aggregated_end_to_end_latency_95pct = agg.end_to_end_latency.get_value_at_percentile(95) / 1000.0
                result.aggregated_end_to_end_latency_99pct = agg.end_to_end_latency.get_value_at_percentile(99) / 1000.0
                result.aggregated_end_to_end_latency_999pct = agg.end_to_end_latency.get_value_at_percentile(99.9) / 1000.0
                result.aggregated_end_to_end_latency_9999pct = agg.end_to_end_latency.get_value_at_percentile(99.99) / 1000.0
                result.aggregated_end_to_end_latency_max = agg.end_to_end_latency.get_max_value() / 1000.0

                # Collect percentiles - define standard percentile list
                percentile_list = [50, 75, 90, 95, 99, 99.9, 99.99]

                for percentile_obj in agg.publish_latency.get_percentile_to_value_dict(percentile_list).items():
                    result.aggregated_publish_latency_quantiles[percentile_obj[0]] = percentile_obj[1] / 1000.0

                for percentile_obj in agg.publish_delay_latency.get_percentile_to_value_dict(percentile_list).items():
                    result.aggregated_publish_delay_latency_quantiles[percentile_obj[0]] = int(percentile_obj[1])

                for percentile_obj in agg.end_to_end_latency.get_percentile_to_value_dict(percentile_list).items():
                    result.aggregated_end_to_end_latency_quantiles[percentile_obj[0]] = self._micros_to_millis(percentile_obj[1])

                break

            old_time = now

        return result

    @staticmethod
    def _micros_to_millis(time_in_micros) -> float:
        """Convert microseconds to milliseconds."""
        return time_in_micros / 1000.0
