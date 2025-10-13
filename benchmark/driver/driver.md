⏺ 这个 driver
  目录是一个消息队列基准测试框架的核心驱动抽象层，各文件功能如下:

  1. __init__.py

  模块初始化文件，导出所有公共接口和类供外部使用。

  2. benchmark_driver.py

  核心驱动抽象基类，定义了消息系统的通用接口:
  - BenchmarkDriver: 抽象基类，所有消息系统驱动的统一接口
    - initialize(): 初始化驱动和客户端库
    - create_topic(): 创建单个主题
    - create_topics(): 批量创建主题
    - create_producer(): 创建生产者
    - create_producers(): 批量创建生产者
    - create_consumer(): 创建消费者
    - create_consumers(): 批量创建消费者
  - 数据类: TopicInfo, ProducerInfo, ConsumerInfo - 封装配置信息

  3. benchmark_producer.py

  生产者抽象接口:
  - BenchmarkProducer: 定义消息发送接口
    - send_async(): 异步发送消息，返回 Future
    - close(): 关闭生产者并清理资源
    - 支持 context manager (with 语句)

  4. benchmark_consumer.py

  消费者抽象接口:
  - BenchmarkConsumer: 简单的消费者接口
    - close(): 关闭消费者并清理资源
    - 支持 context manager

  5. consumer_callback.py

  消费回调接口:
  - ConsumerCallback: 消息接收回调抽象
    - message_received(): 接收消息时调用，传入消息负载和发布时间戳

  6. resource_creator.py

  通用资源创建工具:
  - ResourceCreator: 泛型资源创建器，支持批处理和重试
    - 批量创建资源(生产者/消费者/主题)
    - 支持失败重试机制
    - 批次间延迟控制
    - 周期性日志记录进度
  - CreationResult: 封装创建结果

  设计思想: 这是一个典型的适配器模式框架，通过抽象接口统一不同消息系统(Kafka
  /Pulsar/RabbitMQ等)的基准测试实现，具体的消息系统只需实现这些抽象接口即可
  接入测试框架。

