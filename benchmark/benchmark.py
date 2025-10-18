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

import argparse
import json
import logging
import sys
import yaml
from pathlib import Path
from datetime import datetime
from typing import List, Dict
from benchmark.results_to_csv import ResultsToCsv
from benchmark.workers import Workers
from benchmark.workload import Workload
from benchmark.driver_configuration import DriverConfiguration
from benchmark.workload_generator import WorkloadGenerator

logger = logging.getLogger(__name__)

#   程序接受这些参数：
#   - -d / --drivers：驱动配置文件（如 kafka.yaml）
#   - -w / --workers：工作节点地址列表
#   - -wf / --workers-file：工作节点配置文件
#   - workloads：测试任务配置文件

#   例子：
#   python -m benchmark -d kafka.yaml -wf workers.yaml simple-workload.yaml

class Benchmark:
    """Main benchmark application."""

    @staticmethod
    def main(args: List[str] = None):
        """Main entry point."""
        if args is None:
            args = sys.argv[1:]

        # Parse arguments
        parser = argparse.ArgumentParser(prog="messaging-benchmark")
        parser.add_argument(
            "-c", "--csv",
            dest="results_dir",
            help="Print results from this directory to a csv file"
        )
        parser.add_argument(
            "-d", "--drivers",
            action='append',
            help="Drivers list. eg.: pulsar/pulsar.yaml,kafka/kafka.yaml"
        )
        parser.add_argument(
            "-w", "--workers",
            nargs='+',
            help="List of worker nodes. eg: http://1.2.3.4:8080,http://4.5.6.7:8080"
        )
        parser.add_argument(
            "-wf", "--workers-file",
            dest="workers_file",
            help="Path to a YAML file containing the list of workers addresses"
        )
        parser.add_argument(
            "-x", "--extra",
            dest="extra_consumers",
            action="store_true",
            help="Allocate extra consumer workers when your backlog builds."
        )
        parser.add_argument(
            "workloads",
            nargs='*',
            help="Workloads"
        )
        parser.add_argument(
            "-o", "--output",
            help="Output",
            required=False
        )

        arguments = parser.parse_args(args)

        # Handle CSV export mode
        if arguments.results_dir is not None:
            r = ResultsToCsv()
            r.write_all_result_files(arguments.results_dir)
            sys.exit(0)

        # Validate worker arguments
        if arguments.workers is not None and arguments.workers_file is not None:
            print("Only one between --workers and --workers-file can be specified", file=sys.stderr)
            sys.exit(-1)

        # Load workers from file if needed
        if arguments.workers is None and arguments.workers_file is None:
            default_file = Path("workers.yaml")
            if default_file.exists():
                logger.info("Using default worker file workers.yaml")
                arguments.workers_file = str(default_file)

        if arguments.workers_file is not None:
            logger.info(f"Reading workers list from {arguments.workers_file}")
            with open(arguments.workers_file, 'r') as f:
                workers_obj = yaml.safe_load(f)
                workers = Workers()
                workers.workers = workers_obj.get('workers', [])
                arguments.workers = workers.workers

        # Dump configuration variables
        logger.info(f"Starting benchmark with config: {json.dumps(vars(arguments), indent=2)}")

          # 读取workload配置文件，加载配置
        workloads: Dict[str, Workload] = {}
        for path in arguments.workloads:
            file_path = Path(path)
            name = file_path.stem  # filename without extension

            with open(file_path, 'r') as f:
                workload_data = yaml.safe_load(f)
                workload = Benchmark._dict_to_workload(workload_data)
                workloads[name] = workload

        # Convert workloads to dict for logging, handling enums
        workloads_dict = {}
        for k, v in workloads.items():
            v_dict = vars(v).copy()
            # Convert enum to string if present
            if 'key_distributor' in v_dict and hasattr(v_dict['key_distributor'], 'name'):
                v_dict['key_distributor'] = v_dict['key_distributor'].name
            workloads_dict[k] = v_dict
        logger.info(f"Workloads: {json.dumps(workloads_dict, indent=2)}")

        # Create worker
        from benchmark.worker.local_worker import LocalWorker
        from benchmark.worker.http_worker_client import HttpWorkerClient
        from benchmark.worker.distributed_workers_ensemble import DistributedWorkersEnsemble

        # 用分布模式
        if arguments.workers is not None and len(arguments.workers) > 0:
            workers_list = [HttpWorkerClient(w) for w in arguments.workers]
            worker = DistributedWorkersEnsemble(workers_list, arguments.extra_consumers)
        else:
            # 用本地模式 (ISOLATED多进程架构)
            logger.info("Using LocalWorker (multi-process ISOLATED architecture)")
            worker = LocalWorker()

        # Run benchmarks，顺序执行，不是一起测试的
        for workload_name, workload in workloads.items():
            for driver_config in arguments.drivers:
                try:
                    date_format = "%Y-%m-%d-%H-%M-%S"
                    driver_config_file = Path(driver_config)

                    with open(driver_config_file, 'r') as f:
                        driver_config_data = yaml.safe_load(f)
                        driver_configuration = Benchmark._dict_to_driver_configuration(driver_config_data)

                    logger.info(
                        f"--------------- WORKLOAD : {workload.name} --- DRIVER : {driver_configuration.name}---------------"
                    )

                    # Stop any left over workload
                    worker.stop_all()

                    worker.initialize_driver(str(driver_config_file))

                    generator = WorkloadGenerator(driver_configuration.name, workload, worker)

                    result = generator.run()

                    use_output = (arguments.output is not None) and (len(arguments.output) > 0)

                    if use_output:
                        file_name = arguments.output
                    else:
                        file_name = f"{workload_name}-{driver_configuration.name}-{datetime.now().strftime(date_format)}.json"

                    logger.info(f"Writing test result into {file_name}")
                    with open(file_name, 'w') as f:
                        json.dump(Benchmark._test_result_to_dict(result), f, indent=2)

                    generator.close()

                except Exception as e:
                    logger.error(
                        f"Failed to run the workload '{workload.name}' for driver '{driver_config}'",
                        exc_info=e
                    )
                finally:
                    worker.stop_all()

        worker.close()

    @staticmethod
    def _dict_to_workload(data: dict) -> Workload:
        """Convert dictionary to Workload object."""
        workload = Workload()
        workload.name = data.get('name')
        workload.topics = data.get('topics', 0)
        workload.partitions_per_topic = data.get('partitionsPerTopic', 0)
        workload.message_size = data.get('messageSize', 0)
        workload.use_randomized_payloads = data.get('useRandomizedPayloads', False)
        workload.random_bytes_ratio = data.get('randomBytesRatio', 0.0)
        workload.randomized_payload_pool_size = data.get('randomizedPayloadPoolSize', 0)
        workload.payload_file = data.get('payloadFile')
        workload.subscriptions_per_topic = data.get('subscriptionsPerTopic', 0)
        workload.producers_per_topic = data.get('producersPerTopic', 0)
        workload.consumer_per_subscription = data.get('consumerPerSubscription', 0)
        workload.producer_rate = data.get('producerRate', 0)
        workload.consumer_backlog_size_gb = data.get('consumerBacklogSizeGB', 0)
        workload.backlog_drain_ratio = data.get('backlogDrainRatio', 1.0)
        workload.test_duration_minutes = data.get('testDurationMinutes', 0)
        workload.warmup_duration_minutes = data.get('warmupDurationMinutes', 1)
        workload.message_processing_delay_ms = data.get('messageProcessingDelayMs', 0)

        # Handle key distributor
        key_dist = data.get('keyDistributor', 'NO_KEY')
        if key_dist:
            from benchmark.utils.distributor.key_distributor_type import KeyDistributorType
            workload.key_distributor = KeyDistributorType[key_dist]

        return workload

    @staticmethod
    def _dict_to_driver_configuration(data: dict) -> DriverConfiguration:
        """Convert dictionary to DriverConfiguration object."""
        config = DriverConfiguration()
        config.name = data.get('name')
        config.driver_class = data.get('driverClass')
        return config

    @staticmethod
    def _test_result_to_dict(result) -> dict:
        """Convert TestResult object to dictionary for JSON serialization."""
        return {
            'workload': result.workload,
            'driver': result.driver,
            'messageSize': result.message_size,
            'topics': result.topics,
            'partitions': result.partitions,
            'producersPerTopic': result.producers_per_topic,
            'consumersPerTopic': result.consumers_per_topic,
            'publishRate': result.publish_rate,
            'publishErrorRate': result.publish_error_rate,
            'consumeRate': result.consume_rate,
            'backlog': result.backlog,
            # ⚡ Removed period avg lists - only aggregated avg is meaningful
            'publishLatency50pct': result.publish_latency_50pct,
            'publishLatency75pct': result.publish_latency_75pct,
            'publishLatency95pct': result.publish_latency_95pct,
            'publishLatency99pct': result.publish_latency_99pct,
            'publishLatency999pct': result.publish_latency_999pct,
            'publishLatency9999pct': result.publish_latency_9999pct,
            'publishLatencyMax': result.publish_latency_max,
            'publishDelayLatency50pct': result.publish_delay_latency_50pct,
            'publishDelayLatency75pct': result.publish_delay_latency_75pct,
            'publishDelayLatency95pct': result.publish_delay_latency_95pct,
            'publishDelayLatency99pct': result.publish_delay_latency_99pct,
            'publishDelayLatency999pct': result.publish_delay_latency_999pct,
            'publishDelayLatency9999pct': result.publish_delay_latency_9999pct,
            'publishDelayLatencyMax': result.publish_delay_latency_max,
            'aggregatedPublishLatencyAvg': result.aggregated_publish_latency_avg,
            'aggregatedPublishLatency50pct': result.aggregated_publish_latency_50pct,
            'aggregatedPublishLatency75pct': result.aggregated_publish_latency_75pct,
            'aggregatedPublishLatency95pct': result.aggregated_publish_latency_95pct,
            'aggregatedPublishLatency99pct': result.aggregated_publish_latency_99pct,
            'aggregatedPublishLatency999pct': result.aggregated_publish_latency_999pct,
            'aggregatedPublishLatency9999pct': result.aggregated_publish_latency_9999pct,
            'aggregatedPublishLatencyMax': result.aggregated_publish_latency_max,
            'aggregatedPublishDelayLatencyAvg': result.aggregated_publish_delay_latency_avg,
            'aggregatedPublishDelayLatency50pct': result.aggregated_publish_delay_latency_50pct,
            'aggregatedPublishDelayLatency75pct': result.aggregated_publish_delay_latency_75pct,
            'aggregatedPublishDelayLatency95pct': result.aggregated_publish_delay_latency_95pct,
            'aggregatedPublishDelayLatency99pct': result.aggregated_publish_delay_latency_99pct,
            'aggregatedPublishDelayLatency999pct': result.aggregated_publish_delay_latency_999pct,
            'aggregatedPublishDelayLatency9999pct': result.aggregated_publish_delay_latency_9999pct,
            'aggregatedPublishDelayLatencyMax': result.aggregated_publish_delay_latency_max,
            'aggregatedPublishLatencyQuantiles': result.aggregated_publish_latency_quantiles,
            'aggregatedPublishDelayLatencyQuantiles': result.aggregated_publish_delay_latency_quantiles,
            # ⚡ Removed period avg lists - only aggregated avg is meaningful
            'endToEndLatency50pct': result.end_to_end_latency_50pct,
            'endToEndLatency75pct': result.end_to_end_latency_75pct,
            'endToEndLatency95pct': result.end_to_end_latency_95pct,
            'endToEndLatency99pct': result.end_to_end_latency_99pct,
            'endToEndLatency999pct': result.end_to_end_latency_999pct,
            'endToEndLatency9999pct': result.end_to_end_latency_9999pct,
            'endToEndLatencyMax': result.end_to_end_latency_max,
            'aggregatedEndToEndLatencyQuantiles': result.aggregated_end_to_end_latency_quantiles,
            'aggregatedEndToEndLatencyAvg': result.aggregated_end_to_end_latency_avg,
            'aggregatedEndToEndLatency50pct': result.aggregated_end_to_end_latency_50pct,
            'aggregatedEndToEndLatency75pct': result.aggregated_end_to_end_latency_75pct,
            'aggregatedEndToEndLatency95pct': result.aggregated_end_to_end_latency_95pct,
            'aggregatedEndToEndLatency99pct': result.aggregated_end_to_end_latency_99pct,
            'aggregatedEndToEndLatency999pct': result.aggregated_end_to_end_latency_999pct,
            'aggregatedEndToEndLatency9999pct': result.aggregated_end_to_end_latency_9999pct,
            'aggregatedEndToEndLatencyMax': result.aggregated_end_to_end_latency_max,
            'aggregatedPublishRateAvg': result.aggregated_publish_rate_avg,
            'aggregatedConsumeRateAvg': result.aggregated_consume_rate_avg,
            'aggregatedPublishThroughputAvg': result.aggregated_publish_throughput_avg,
            'aggregatedConsumeThroughputAvg': result.aggregated_consume_throughput_avg,
            'aggregatedMessagesSent': result.aggregated_messages_sent,
            'aggregatedMessagesReceived': result.aggregated_messages_received,
        }


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    Benchmark.main()
