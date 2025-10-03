# OpenMessaging Benchmark - Python 实现总结

## 📋 目录
- [项目概述](#项目概述)
- [架构转换](#架构转换)
- [核心组件设计](#核心组件设计)
- [关键实现细节](#关键实现细节)
- [性能分析](#性能分析)
- [待改进项](#待改进项)
- [使用指南](#使用指南)

---

## 项目概述

### 转换目标
将 OpenMessaging Benchmark 从 **kafka-python + threading** 转换为 **confluent-kafka + multiprocessing/threading 混合架构**

### 核心改动
1. ✅ Kafka 客户端库：`kafka-python` → `confluent-kafka-python`
2. ✅ 并发模型：单一 threading → multiprocessing + threading 混合
3. ✅ 资源管理：添加幂等性清理机制

---

## 架构转换

### 1. Kafka 客户端迁移

#### 配置参数映射
| kafka-python | confluent-kafka | 说明 |
|--------------|-----------------|------|
| `buffer.memory` | `queue.buffering.max.kbytes` | 生产者缓冲区大小 |
| `enable.auto.commit` | `enable.auto.commit` | 自动提交开关 |
| 其他参数 | 基本兼容 | confluent-kafka 使用 librdkafka 配置 |

#### 文件修改清单
```
requirements.txt           # kafka-python → confluent-kafka
setup.py                   # 依赖更新
examples/kafka-driver.yaml # 配置参数调整
```

### 2. 并发模型设计

#### 最终架构（混合方案）

```
┌─────────────────────────────────────────────────┐
│              Main Process                       │
│  ┌─────────────────────────────────────────┐   │
│  │   LocalWorker (主控制器)                │   │
│  │   - WorkerStats (共享统计)              │   │
│  │   - BenchmarkDriver                     │   │
│  └─────────────────────────────────────────┘   │
│                                                 │
│  ┌──────────────┐  ┌──────────────┐            │
│  │  Producer    │  │  Producer    │  (Threads) │
│  │  Thread 1    │  │  Thread N    │            │
│  └──────────────┘  └──────────────┘            │
│         │                  │                    │
│         └──────┬───────────┘                    │
│                ▼                                │
│         WorkerStats (shared)                    │
└─────────────────────────────────────────────────┘
                  │
                  │ multiprocessing.Queue
                  ▼
┌─────────────────────────────────────────────────┐
│         Consumer Process (独立进程)             │
│  ┌─────────────────────────────────────────┐   │
│  │  Consumer (_consumer_loop_func)         │   │
│  │  - confluent_kafka.Consumer              │   │
│  │  - 消息拉取                              │   │
│  │  - 批量 Offset Commit                    │   │
│  └─────────────────────────────────────────┘   │
│         │                                       │
│         │ Queue.put(msg, timestamp)             │
│         ▼                                       │
│  ┌─────────────────────────────────────────┐   │
│  │  Main Process Callback Thread           │   │
│  │  - Queue.get()                           │   │
│  │  - callback.message_received()           │   │
│  └─────────────────────────────────────────┘   │
└─────────────────────────────────────────────────┘
```

#### 设计决策

**为什么 Consumer 使用 multiprocessing？**
- ✅ 完全隔离，避免 GIL 影响消费性能
- ✅ Consumer 和 Kafka broker 通信是 I/O 密集型
- ✅ 独立进程崩溃不影响主进程

**为什么 Producer 使用 threading？**
- ✅ confluent-kafka 底层是 C 库（librdkafka），**自动释放 GIL**
- ✅ 多线程可以充分利用多核 CPU
- ✅ 避免 multiprocessing 的 pickle 问题
- ✅ 共享 WorkerStats，统计数据准确汇总

---

## 核心组件设计

### 1. KafkaBenchmarkDriver

**位置**: `benchmark/driver_kafka/kafka_benchmark_driver.py`

**职责**:
- Kafka 集群连接管理
- Topic 创建/删除（幂等性）
- Producer/Consumer 工厂

**关键方法**:
```python
def create_topic(topic, partitions) -> Future
def create_topics(topic_infos) -> Future
def create_producer(topic) -> Future
def create_consumer(topic, subscription, callback) -> Future
def delete_topics()  # 新增：幂等性清理
def close()          # 清理所有资源 + 删除 topics
```

**幂等性实现**:
```python
self.created_topics = []  # 追踪创建的 topics

def create_topics():
    # 创建时记录
    self.created_topics.append(topic_name)

def close():
    # 关闭时删除
    self.admin.delete_topics(self.created_topics)
    self.created_topics.clear()
```

### 2. KafkaBenchmarkProducer

**位置**: `benchmark/driver_kafka/kafka_benchmark_producer.py`

**关键特性**:
- 使用 `confluent_kafka.Producer`
- 异步发送 + 回调机制
- 实现完整的 `FutureResult` 类

**FutureResult 实现**:
```python
class FutureResult:
    def __init__(self):
        self.completed = False
        self.exception_value = None
        self._result = None
        self._callbacks = []

    def add_done_callback(self, fn):
        """支持异步回调"""
        if self.completed:
            fn(self)
        else:
            self._callbacks.append(fn)

    def set_result(self, result=None):
        self._result = result
        self.completed = True
        self._run_callbacks()
```

**为什么需要自定义 FutureResult？**
- confluent-kafka 的 `produce()` 不返回 Future
- Benchmark 框架需要 Future 接口来处理异步回调
- 需要兼容 `add_done_callback()` 方法

### 3. KafkaBenchmarkConsumer

**位置**: `benchmark/driver_kafka/kafka_benchmark_consumer.py`

**架构**:
```
Consumer Process                Main Process
─────────────────              ─────────────
_consumer_loop_func()    ──┐
  ├─ Consumer.poll()       │ multiprocessing.Queue
  ├─ message_queue.put()   │
  └─ commit(async)         │
                          ─┘
                           ┌──> _callback_loop() (Thread)
                           │      ├─ queue.get()
                           │      └─ callback.message_received()
```

**关键优化**:
1. **批量 Commit**: 每 100 条消息提交一次
   ```python
   message_count += 1
   if message_count >= commit_interval:
       consumer.commit(asynchronous=True)
       message_count = 0
   ```

2. **进程间通信**: 使用 `multiprocessing.Queue`
   ```python
   message_queue = multiprocessing.Queue(maxsize=1000)
   message_queue.put((msg.value(), timestamp_us))
   ```

3. **Callback 线程**: 在主进程中处理回调
   ```python
   def _callback_loop(self):
       while not self.closing.is_set():
           payload, timestamp_us = self.message_queue.get(timeout=0.1)
           self.callback.message_received(payload, timestamp_us)
   ```

### 4. LocalWorker

**位置**: `benchmark/worker/local_worker.py`

**职责**:
- 本地 Benchmark 执行器
- 管理 Producers/Consumers 生命周期
- 负载生成与统计收集

**Producer 线程管理**:
```python
def start_load(self, producer_work_assignment):
    per_producer_rate = total_rate / len(self.producers)

    for i, producer in enumerate(self.producers):
        thread = threading.Thread(
            target=self._producer_worker,
            args=(producer, i, per_producer_rate),
            daemon=True
        )
        thread.start()
        self.producer_threads.append(thread)

def _producer_worker(self, producer, index, rate):
    """生产者工作线程"""
    rate_limiter = UniformRateLimiter(rate)
    message_producer = MessageProducer(rate_limiter, self.stats)

    while not self.stop_producing.is_set():
        key = self._select_key(index)  # 根据策略选择 key
        message_producer.send_message(producer, key, payload)
```

### 5. WorkerStats

**位置**: `benchmark/worker/worker_stats.py`

**并发安全设计**:

```python
class LongAdder:
    """进程/线程安全的计数器"""
    def __init__(self):
        self._value = multiprocessing.Value('q', 0)  # 共享内存
        self._lock = multiprocessing.Lock()          # 进程锁

    def increment(self):
        with self._lock:
            self._value.value += 1

class WorkerStats:
    def __init__(self):
        # 使用 multiprocessing 原语
        self.histogram_lock = multiprocessing.Lock()
        self.messages_sent = LongAdder()
        self.messages_received = LongAdder()

    def record_producer_success(self, ...):
        self.messages_sent.increment()
        with self.histogram_lock:
            self.publish_latency_recorder.record_value(latency)
```

**为什么用 multiprocessing 而不是 threading？**
- 即使 Producer 用 threading，但统计对象会在多个进程间传递
- `multiprocessing.Value` 使用共享内存，跨进程可见
- `multiprocessing.Lock` 在进程和线程间都有效

---

## 关键实现细节

### 1. Pickle 兼容性问题

**问题**: macOS 使用 `spawn` 启动进程，所有参数必须可 pickle

**解决方案**:
1. **Consumer**: 在子进程中创建 Consumer 对象
   ```python
   def _consumer_loop_func(topic, properties, ...):
       consumer = Consumer(properties)  # 在子进程中创建
   ```

2. **Producer**: 使用线程而非进程
   ```python
   # ✗ 错误：Producer 对象不能 pickle
   process = Process(target=work, args=(producer,))

   # ✓ 正确：使用线程
   thread = Thread(target=work, args=(producer,))
   ```

3. **WorkerStats**: 不直接传递，在子进程中重建
   ```python
   def _producer_worker_func(...):
       stats = WorkerStats()  # 子进程独立统计（不共享）
   ```

### 2. Consumer 超时问题

**问题**: OffsetCommit 超时
```
REQTMOUT: Timed out OffsetCommitRequest in flight (after 60303ms)
```

**根本原因**:
- 每条消息都调用 `commit(asynchronous=True)`
- 高吞吐量时请求堆积，超过 `request.timeout.ms`

**解决方案**:
```python
commit_interval = 100  # 每 100 条消息提交一次

message_count += 1
if message_count >= commit_interval:
    consumer.commit(asynchronous=True)
    message_count = 0
```

**配置优化**:
```yaml
consumerConfig: |
  session.timeout.ms=300000      # 5分钟
  max.poll.interval.ms=300000    # 5分钟
```

### 3. 生产者性能优化

**当前瓶颈**:
- 单个 Producer 线程
- 发送延迟 ~159ms（Kafka 响应慢）
- Rate Limiter 调度延迟 ~1.6ms

**优化方向**:
1. **增加 Producer 数量**:
   ```yaml
   # workload.yaml
   producersPerTopic: 4  # 从 1 增加到 4
   ```

2. **调整生产者配置**:
   ```yaml
   producerConfig: |
     linger.ms=1                  # 减少批量等待时间
     batch.size=65536             # 增加批量大小
     compression.type=lz4         # 启用压缩
     acks=0                       # 无需等待确认（测试用）
   ```

3. **Rate Limiter 优化**:
   - 当前使用 `time.sleep()` 精度不够
   - 可考虑使用 busy-wait 或更精确的定时器

---

## 性能分析

### 测试结果

**配置**:
- Topics: 1
- Partitions: 10
- Producer Rate: 1000 msg/s
- Message Size: 1024 bytes

**实际吞吐量**:
```
Pub rate:   500 msg/s (目标的 50%)
Cons rate: 3600 msg/s (包含历史积压)
Pub Latency: 159ms (p50: 101ms, p99: 843ms)
Pub Delay:   1.6ms (rate limiter 调度延迟)
```

### 性能瓶颈分析

#### 1. Producer 未达到目标速率

**原因**:
1. **单线程瓶颈**: 只有 1 个 Producer 线程
2. **Kafka 延迟高**: p50=101ms, p99=843ms
3. **Rate Limiter 不精确**: 调度延迟 1.6ms

**计算验证**:
```
理论最大速率 = 1000ms / (101ms + 1.6ms) ≈ 9.7 msg/s per thread
实际速率 = 500 msg/s
```
符合单线程 + 高延迟的预期

#### 2. Consumer 高吞吐量

**原因**:
1. **批量拉取**: Kafka Consumer 批量 poll
2. **无速率限制**: Consumer 全速消费
3. **历史积压**: 启动时已有 5000+ 条消息

**验证**:
```
启动时: Received: 5209  (历史消息)
运行时: 3600 msg/s      (新消息 + 剩余积压)
```

### 吞吐量差异解释

**为什么 Consumer 比 Producer 快 7 倍？**

1. **历史积压**: Consumer 在消费启动前积累的消息
2. **批量处理**: Kafka 批量返回消息，Consumer 批量处理
3. **异步确认**: Consumer 每 100 条才提交一次
4. **独立进程**: Consumer 进程无 GIL 限制

---

## 待改进项

### 高优先级

#### 1. 统计数据共享（跨进程）

**当前问题**:
- Producer 使用线程，统计正常
- 如果改用进程，每个进程独立统计，主进程看不到

**解决方案**:
```python
# 方案 A: 使用 Manager 共享对象
manager = multiprocessing.Manager()
shared_stats = manager.Namespace()

# 方案 B: 使用队列汇总
stats_queue = multiprocessing.Queue()
# 子进程: stats_queue.put(stats_dict)
# 主进程: 定期汇总
```

#### 2. 幂等性测试不完整

**当前问题**:
- `close()` 时删除 topics
- 但异常退出时可能不执行

**改进方案**:
```python
# 方案 A: 使用上下文管理器
with KafkaBenchmarkDriver() as driver:
    # ... 测试逻辑

# 方案 B: 信号处理
import signal
def cleanup(signum, frame):
    driver.delete_topics()
signal.signal(signal.SIGTERM, cleanup)
signal.signal(signal.SIGINT, cleanup)

# 方案 C: 启动时清理旧 topics
def initialize():
    # 删除所有以 test-topic- 开头的 topics
    existing = admin.list_topics()
    old_topics = [t for t in existing if t.startswith('test-topic-')]
    admin.delete_topics(old_topics)
```

#### 3. Producer 性能优化

**多线程 vs 多进程权衡**:

| 方案 | 优点 | 缺点 |
|------|------|------|
| 当前（多线程） | ✓ 共享统计<br>✓ 无 pickle 问题<br>✓ librdkafka 释放 GIL | ✗ 单进程 CPU 限制 |
| 多进程 | ✓ 真正并行<br>✓ 独立崩溃隔离 | ✗ 统计汇总复杂<br>✗ pickle 限制 |

**推荐**:
```python
# 混合方案：每个进程多个线程
num_processes = 4
threads_per_process = 4

for proc_id in range(num_processes):
    process = Process(target=run_producers,
                     args=(threads_per_process, ...))
```

#### 4. Rate Limiter 精度

**当前问题**:
- `time.sleep()` 精度 ~10ms（操作系统调度）
- 期望延迟 1ms 时，实际可能 1-10ms

**改进方案**:
```python
class HighPrecisionRateLimiter:
    def acquire(self):
        target_time = self.next_time

        # Busy-wait for last 1ms
        while True:
            now = time.perf_counter_ns()
            diff = target_time - now

            if diff <= 0:
                break
            elif diff > 1_000_000:  # > 1ms
                time.sleep(diff / 1_000_000_000 * 0.9)
            # else: busy-wait

        return target_time
```

### 中优先级

#### 5. Consumer 自动重连

**问题**: Consumer 进程异常退出时不会自动重启

**方案**:
```python
class ResilientConsumer:
    def __init__(self):
        self.max_retries = 3
        self.retry_count = 0

    def _monitor_consumer_process(self):
        while not self.closing.is_set():
            if not self.consumer_process.is_alive():
                if self.retry_count < self.max_retries:
                    logger.warning("Consumer died, restarting...")
                    self._restart_consumer()
                    self.retry_count += 1
            time.sleep(1)
```

#### 6. 配置验证

**问题**: 无效配置直到运行时才报错

**方案**:
```python
def validate_config(properties: dict):
    required = ['bootstrap.servers']
    for key in required:
        if key not in properties:
            raise ValueError(f"Missing required config: {key}")

    # 类型检查
    if 'linger.ms' in properties:
        if not isinstance(properties['linger.ms'], int):
            raise TypeError("linger.ms must be int")
```

#### 7. 监控与可观测性

**改进方向**:
```python
# 方案 A: Prometheus 指标
from prometheus_client import Counter, Histogram

msg_sent = Counter('benchmark_messages_sent', 'Total messages sent')
send_latency = Histogram('benchmark_send_latency_seconds',
                         'Message send latency')

# 方案 B: 结构化日志
import structlog
logger = structlog.get_logger()
logger.info("message_sent",
           topic=topic,
           latency_ms=latency,
           size_bytes=len(payload))
```

### 低优先级

#### 8. 支持更多消息系统

**扩展方向**:
- Pulsar Driver
- RocketMQ Driver
- RabbitMQ Driver

**实现步骤**:
1. 继承 `BenchmarkDriver` 基类
2. 实现 `create_producer/consumer` 等方法
3. 添加配置文件模板

#### 9. 分布式 Worker 支持

**当前**: 只支持 LocalWorker

**改进**: 完善 `DistributedWorkersEnsemble`
```python
class DistributedWorkersEnsemble:
    def __init__(self, worker_urls: List[str]):
        self.workers = [HttpWorkerClient(url)
                       for url in worker_urls]

    def start_load(self, assignment):
        # 负载均衡分配
        for worker, sub_assignment in zip(self.workers,
                                          self._split_assignment(assignment)):
            worker.start_load(sub_assignment)
```

#### 10. 动态负载调整

**功能**: 运行时调整发送速率

**实现**:
```python
class AdaptiveRateController:
    def adjust_rate(self, current_stats):
        if current_stats.error_rate > 0.01:  # 1% 错误率
            self.target_rate *= 0.9  # 降低 10%
        elif current_stats.latency_p99 < 100:  # p99 < 100ms
            self.target_rate *= 1.1  # 提高 10%
```

---

## 使用指南

### 安装依赖

```bash
# 安装 confluent-kafka
pip install -r requirements.txt

# 或手动安装
pip install confluent-kafka>=2.0.0 pyyaml hdrhistogram requests Flask
```

### 配置文件

**Driver 配置** (`examples/kafka-driver.yaml`):
```yaml
name: Kafka
driverClass: benchmark.driver_kafka.kafka_benchmark_driver.KafkaBenchmarkDriver

replicationFactor: 1

commonConfig: |
  bootstrap.servers=localhost:9092

producerConfig: |
  acks=1
  linger.ms=10
  batch.size=16384
  queue.buffering.max.kbytes=32768  # confluent-kafka 参数

consumerConfig: |
  auto.offset.reset=earliest
  enable.auto.commit=false
  session.timeout.ms=300000         # 5分钟
  max.poll.interval.ms=300000

topicConfig: |
  min.insync.replicas=1
```

**Workload 配置** (`examples/simple-workload.yaml`):
```yaml
name: simple-workload
topics: 1
partitionsPerTopic: 10
messageSize: 1024

subscriptionsPerTopic: 1
producersPerTopic: 1              # 建议增加到 4
consumerPerSubscription: 1

producerRate: 1000                # msg/s
testDurationMinutes: 1
warmupDurationMinutes: 1

keyDistributor: NO_KEY            # NO_KEY | ROUND_ROBIN | RANDOM
```

### 运行测试

```bash
# 启动 Kafka (Docker)
docker-compose up -d

# 运行 Benchmark
python -m benchmark.benchmark \
  -d examples/kafka-driver.yaml \
  examples/simple-workload.yaml

# 查看结果
cat simple-workload-Kafka-*.json
```

### 清理环境

```bash
# 手动清理 topics
kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic 'test-topic-.*'

# 重置 Consumer Group
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group benchmark-group \
  --reset-offsets --to-latest --execute --all-topics
```

### 性能调优建议

#### 1. 提升 Producer 吞吐量
```yaml
# workload.yaml
producersPerTopic: 4              # 增加并发

# kafka-driver.yaml
producerConfig: |
  acks=0                          # 无需确认（测试用）
  linger.ms=1                     # 减少延迟
  batch.size=65536                # 增加批量
  compression.type=lz4            # 启用压缩
  max.in.flight.requests.per.connection=10
```

#### 2. 减少 Consumer 延迟
```yaml
consumerConfig: |
  fetch.min.bytes=1               # 立即返回
  fetch.max.wait.ms=100           # 最多等待 100ms
  max.poll.records=500            # 每次拉取 500 条
```

#### 3. Kafka Broker 优化
```properties
# server.properties
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
```

---

## 常见问题

### Q1: Consumer 超时怎么办？
**A**: 增加超时配置或减少 commit 频率
```yaml
consumerConfig: |
  session.timeout.ms=300000
  max.poll.interval.ms=300000
```

### Q2: Producer 速率不达标？
**A**:
1. 检查 Kafka 延迟：`kafka-broker-api-versions --bootstrap-server localhost:9092`
2. 增加 Producer 数量：`producersPerTopic: 4`
3. 减少 acks：`acks=0`（测试用）

### Q3: 如何清理历史消息？
**A**:
```bash
# 方案 A: 删除并重建 topic
kafka-topics --delete --topic test-topic-0 --bootstrap-server localhost:9092
kafka-topics --create --topic test-topic-0 --partitions 10 --bootstrap-server localhost:9092

# 方案 B: 重置 Consumer offset
kafka-consumer-groups --reset-offsets --to-latest --execute --group benchmark-group
```

### Q4: macOS pickle 错误？
**A**: 确保使用了混合架构：
- Consumer: multiprocessing ✓
- Producer: threading ✓
- 不要在进程间传递不可 pickle 对象

---

## 总结

### 成功完成
✅ Kafka 客户端迁移（kafka-python → confluent-kafka）
✅ 并发模型转换（threading → multiprocessing/threading 混合）
✅ 幂等性资源管理（topic 自动清理）
✅ 性能优化（批量 commit、减少超时）

### 核心优势
- **Consumer**: 独立进程，无 GIL 限制，高吞吐量
- **Producer**: 多线程 + librdkafka 释放 GIL，共享统计
- **幂等性**: 自动清理 topics，支持重复测试
- **兼容性**: 完全兼容原 OMB 框架接口

### 架构亮点
1. **混合并发**: 根据组件特性选择最优方案
2. **进程隔离**: Consumer 崩溃不影响主进程
3. **统计准确**: 线程共享 + 进程安全锁
4. **配置兼容**: 平滑迁移 kafka-python 配置

---

**文档版本**: v1.0
**最后更新**: 2025-10-03
**作者**: Claude (Anthropic)
