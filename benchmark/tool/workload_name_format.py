from string import Template


class WorkloadNameFormat:
    """
    Generates Workload names based on a template. Substitutes template place-holders of the
    form ${variableName}, where variableName is the name of a public member in Workload.
    Note that the set of variables is statically assigned. Numeric values will typically
    be in a form that includes an SI suffix.
    """

    MAX_PRODUCER_RATE = 10_000_000

    def __init__(self, format_str: str):
        """
        Initialize WorkloadNameFormat.

        :param format_str: Format template string
        """
        self.format = format_str

    def from_workload(self, workload) -> str:
        """
        Generate name from workload.

        :param workload: Workload object
        :return: Formatted name
        """
        if workload.name is not None:
            return workload.name

        params = {}
        params['topics'] = self._count_to_display_size(workload.topics)
        params['partitionsPerTopic'] = self._count_to_display_size(workload.partitions_per_topic)
        params['messageSize'] = self._count_to_display_size(workload.message_size)
        params['subscriptionsPerTopic'] = self._count_to_display_size(workload.subscriptions_per_topic)
        params['producersPerTopic'] = self._count_to_display_size(workload.producers_per_topic)
        params['consumerPerSubscription'] = self._count_to_display_size(workload.consumer_per_subscription)

        if workload.producer_rate >= self.MAX_PRODUCER_RATE:
            params['producerRate'] = 'max-rate'
        else:
            params['producerRate'] = self._count_to_display_size(workload.producer_rate)

        params['keyDistributor'] = workload.key_distributor
        params['payloadFile'] = workload.payload_file
        params['useRandomizedPayloads'] = workload.use_randomized_payloads
        params['randomBytesRatio'] = workload.random_bytes_ratio
        params['randomizedPayloadPoolSize'] = self._count_to_display_size(workload.randomized_payload_pool_size)
        params['consumerBacklogSizeGB'] = self._count_to_display_size(workload.consumer_backlog_size_gb)
        params['testDurationMinutes'] = workload.test_duration_minutes
        params['warmupDurationMinutes'] = workload.warmup_duration_minutes

        return Template(self.format).substitute(params)

    @staticmethod
    def _count_to_display_size(size: int) -> str:
        """
        Convert count to display size with SI suffix.

        :param size: Size value
        :return: Display string with suffix
        """
        if size // 1_000_000_000 > 0:
            display_size = str(size // 1_000_000_000) + "g"
        elif size // 1_000_000 > 0:
            display_size = str(size // 1_000_000) + "m"
        elif size // 1_000 > 0:
            display_size = str(size // 1_000) + "k"
        else:
            display_size = str(size)

        return display_size
