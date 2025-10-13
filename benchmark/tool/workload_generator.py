import logging
from typing import List
from .workload_set_template import WorkloadSetTemplate
from .workload_name_format import WorkloadNameFormat

logger = logging.getLogger(__name__)


class WorkloadGenerator:
    """
    Expands a WorkloadSetTemplate into a set of Workloads.
    """

    def __init__(self, template: WorkloadSetTemplate):
        """
        Initialize WorkloadGenerator.

        :param template: WorkloadSetTemplate object
        """
        if template is None:
            raise ValueError("template cannot be None")

        self.template = template
        self.name_format = WorkloadNameFormat(template.name_format)

    def generate(self) -> List:
        """
        Generate list of workloads.

        :return: List of Workload objects
        """
        workloads = []

        # Create base workload with common properties
        from benchmark.workload import Workload

        workload = Workload()
        workload.key_distributor = self.template.key_distributor
        workload.payload_file = self.template.payload_file
        workload.random_bytes_ratio = self.template.random_bytes_ratio
        workload.randomized_payload_pool_size = self.template.randomized_payload_pool_size
        workload.consumer_backlog_size_gb = self.template.consumer_backlog_size_gb
        workload.test_duration_minutes = self.template.test_duration_minutes
        workload.warmup_duration_minutes = self.template.warmup_duration_minutes
        workload.use_randomized_payloads = self.template.use_randomized_payloads

        # Generate cartesian product of all parameter combinations
        for t in self.template.topics:
            for pa in self.template.partitions_per_topic:
                for ms in self.template.message_size:
                    for pd in self.template.producers_per_topic:
                        for st in self.template.subscriptions_per_topic:
                            for cn in self.template.consumer_per_subscription:
                                for pr in self.template.producer_rate:
                                    workload.topics = t
                                    workload.partitions_per_topic = pa
                                    workload.message_size = ms
                                    workload.producers_per_topic = pd
                                    workload.subscriptions_per_topic = st
                                    workload.consumer_per_subscription = cn
                                    workload.producer_rate = pr

                                    copy = self._copy_of(workload)
                                    workloads.append(copy)
                                    logger.info(f"Generated: {copy.name}")

        logger.info(f"Generated {len(workloads)} workloads.")
        return workloads

    def _copy_of(self, workload) -> object:
        """
        Create a copy of workload.

        :param workload: Workload object
        :return: Copy of workload
        """
        from benchmark.workload import Workload

        copy = Workload()
        copy.key_distributor = workload.key_distributor
        copy.payload_file = workload.payload_file
        copy.random_bytes_ratio = workload.random_bytes_ratio
        copy.randomized_payload_pool_size = workload.randomized_payload_pool_size
        copy.consumer_backlog_size_gb = workload.consumer_backlog_size_gb
        copy.test_duration_minutes = workload.test_duration_minutes
        copy.warmup_duration_minutes = workload.warmup_duration_minutes
        copy.topics = workload.topics
        copy.partitions_per_topic = workload.partitions_per_topic
        copy.message_size = workload.message_size
        copy.producers_per_topic = workload.producers_per_topic
        copy.subscriptions_per_topic = workload.subscriptions_per_topic
        copy.consumer_per_subscription = workload.consumer_per_subscription
        copy.producer_rate = workload.producer_rate
        copy.use_randomized_payloads = workload.use_randomized_payloads
        copy.name = self.name_format.from_workload(copy)

        return copy
