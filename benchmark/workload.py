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

        # 消息处理延迟（毫秒），用于模拟慢速消费者
        # 0 表示无延迟（默认），> 0 表示每条消息处理后等待指定毫秒数
        # 实际延迟 = 批量消息数 * message_processing_delay_ms
        self.message_processing_delay_ms = 0
