import argparse
import logging
import os
import sys
import yaml
import json
from pathlib import Path
from .workload_set_template import WorkloadSetTemplate
from .workload_generator import WorkloadGenerator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WorkloadGenerationTool:
    """Generates a set of Workload definition files from a WorkloadSetTemplate file."""

    @staticmethod
    def main(args=None):
        """
        Main entry point.

        :param args: Command line arguments
        """
        parser = argparse.ArgumentParser(
            prog='workload-generator',
            description='Generate workload definition files from a template'
        )

        parser.add_argument(
            '-t', '--template-file',
            type=str,
            required=True,
            help='Path to a YAML file containing the workload template'
        )

        parser.add_argument(
            '-o', '--output-folder',
            type=str,
            required=True,
            help='Output folder for generated workload files'
        )

        parser.add_argument(
            '-h', '--help',
            action='help',
            help='Show this help message and exit'
        )

        parsed_args = parser.parse_args(args)

        # Dump configuration variables
        logger.info(f"Starting benchmark with config: {json.dumps(vars(parsed_args))}")

        # Load template
        template_file = Path(parsed_args.template_file)
        if not template_file.exists():
            logger.error(f"Template file not found: {template_file}")
            sys.exit(1)

        with open(template_file, 'r') as f:
            template_data = yaml.safe_load(f)

        # Convert dict to WorkloadSetTemplate
        template = WorkloadSetTemplate(**template_data)

        # Generate workloads
        workloads = WorkloadGenerator(template).generate()

        # Create output folder
        output_folder = Path(parsed_args.output_folder)
        output_folder.mkdir(parents=True, exist_ok=True)

        # Write workload files
        for w in workloads:
            output_file = None
            try:
                output_file = output_folder / f"{w.name}.yaml"

                # Convert workload to dict for YAML serialization
                workload_dict = {
                    'name': w.name,
                    'topics': w.topics,
                    'partitionsPerTopic': w.partitions_per_topic,
                    'messageSize': w.message_size,
                    'subscriptionsPerTopic': w.subscriptions_per_topic,
                    'producersPerTopic': w.producers_per_topic,
                    'consumerPerSubscription': w.consumer_per_subscription,
                    'producerRate': w.producer_rate,
                    'keyDistributor': w.key_distributor,
                    'payloadFile': w.payload_file,
                    'useRandomizedPayloads': w.use_randomized_payloads,
                    'randomBytesRatio': w.random_bytes_ratio,
                    'randomizedPayloadPoolSize': w.randomized_payload_pool_size,
                    'consumerBacklogSizeGB': w.consumer_backlog_size_gb,
                    'testDurationMinutes': w.test_duration_minutes,
                    'warmupDurationMinutes': w.warmup_duration_minutes
                }

                with open(output_file, 'w') as f:
                    yaml.dump(workload_dict, f, default_flow_style=False, sort_keys=False)

            except IOError as e:
                logger.error(f"Could not write file: {output_file}", exc_info=True)


if __name__ == '__main__':
    WorkloadGenerationTool.main()
