class Workload:

    def __init__(self):
        self.name = None

        # Number of topics to create in the test.
        self.topics = 0

        # Number of partitions each topic will contain.
        self.partitions_per_topic = 0

        self.key_distributor = None  # KeyDistributorType.NO_KEY

        self.message_size = 0

        self.use_randomized_payloads = False

        self.random_bytes_ratio = 0.0
        
        self.randomized_payload_pool_size = 0

        self.payload_file = None

        self.subscriptions_per_topic = 0

        self.producers_per_topic = 0

        self.consumer_per_subscription = 0

        self.producer_rate = 0

        self.consumer_backlog_size_gb = 0

        self.backlog_drain_ratio = 1.0

        self.test_duration_minutes = 0

        self.warmup_duration_minutes = 1
