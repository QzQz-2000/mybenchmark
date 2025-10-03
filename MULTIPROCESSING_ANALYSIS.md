# Python 多进程实现分析 - Agent 模拟

## 🎯 你的设计意图

> **目标**: 使用多进程模拟多个 agent 同时运行

这是一个**完全合理**的设计！让我分析当前实现的问题和改进方向。

---

## 📊 当前实现状态

### Java 版本 - 多线程模拟

```java
public class LocalWorker {
    private final ExecutorService executor =
        Executors.newCachedThreadPool();  // ✓ 线程池

    @Override
    public void startLoad(ProducerWorkAssignment assignment) {
        int processors = Runtime.getRuntime().availableProcessors();  // ✓ CPU 核数

        // ✓ 将 producers 分配到不同的 "processor"
        Map<Integer, List<BenchmarkProducer>> processorAssignment = new TreeMap<>();
        int processorIdx = 0;
        for (BenchmarkProducer p : producers) {
            processorAssignment
                .computeIfAbsent(processorIdx, x -> new ArrayList<>())
                .add(p);
            processorIdx = (processorIdx + 1) % processors;  // 轮询分配
        }

        // ✓ 每个 "processor" 一个线程，内部处理多个 producers
        processorAssignment.values().forEach(producers ->
            submitProducersToExecutor(producers, keyDistributor, payloads)
        );
    }

    private void submitProducersToExecutor(
            List<BenchmarkProducer> producers, ...) {
        executor.submit(() -> {  // ✓ 提交一个任务到线程池
            while (!testCompleted) {
                producers.forEach(p ->  // ✓ 循环所有 producer
                    messageProducer.sendMessage(p, key, payload)
                );
            }
        });
    }
}
```

**Java 版本的设计**:
1. ✅ 根据 CPU 核数分配 producer
2. ✅ 每个核一个线程 = **模拟多个 agent**
3. ✅ 每个线程内循环处理多个 producer
4. ✅ 使用 `CachedThreadPool` 管理线程

---

### Python 版本 - 当前实现（有问题）

```python
class LocalWorker:
    def start_load(self, producer_work_assignment):
        # ❌ 问题：每个 producer 一个线程，而不是每个 CPU 核一个
        self.producer_threads = []
        for i, producer in enumerate(self.producers):
            thread = threading.Thread(
                target=self._producer_worker,
                args=(producer, i, per_producer_rate),
                daemon=True
            )
            thread.start()
            self.producer_threads.append(thread)

    def _producer_worker(self, producer, producer_index, publish_rate):
        """一个线程处理一个 producer"""  # ❌ 错误设计
        rate_limiter = UniformRateLimiter(publish_rate)
        message_producer = MessageProducer(rate_limiter, self.stats)

        while not self.stop_producing.is_set():
            # 只处理这一个 producer
            message_producer.send_message(producer, key, payload)
```

**Python 版本的问题**:
1. ❌ **没有按 CPU 核数分配**
2. ❌ 每个 producer 一个线程（如果有 100 个 producer = 100 个线程）
3. ❌ 没有实现"多 agent 模拟"的概念
4. ❌ 未使用 `_producer_worker_func`（进程版本）

---

## 🔧 正确的多进程实现

### 方案 A: 完全对应 Java 版本（推荐）

```python
import multiprocessing
import random
from typing import List

class LocalWorker:
    def __init__(self):
        self.benchmark_driver = None
        self.producers = []
        self.consumers = []
        self.stats = WorkerStats()
        self.test_completed = multiprocessing.Event()
        self.producer_processes = []  # ✓ 进程列表

    def start_load(self, producer_work_assignment):
        """模拟多个 agent (进程)"""
        cpu_count = multiprocessing.cpu_count()  # ✓ 获取 CPU 核数

        logger.info(f"Starting load with {cpu_count} agent processes")

        # ✓ 将 producers 分配到不同的 "agent" (进程)
        processor_assignment = {}
        processor_idx = 0
        for i, producer in enumerate(self.producers):
            if processor_idx not in processor_assignment:
                processor_assignment[processor_idx] = []

            # 保存 producer 信息（topic + properties）
            processor_assignment[processor_idx].append({
                'topic': producer.topic,
                'index': i
            })

            processor_idx = (processor_idx + 1) % cpu_count  # 轮询分配

        # ✓ 为每个 CPU 核启动一个进程（模拟一个 agent）
        producer_properties = self.benchmark_driver.producer_properties.copy()

        for proc_id, producer_infos in processor_assignment.items():
            process = multiprocessing.Process(
                target=_agent_worker_func,  # ✓ 全局函数
                args=(
                    proc_id,                          # agent ID
                    producer_infos,                   # 分配的 producers
                    producer_properties,              # Kafka 配置
                    producer_work_assignment,         # 工作负载
                    self.test_completed,              # 停止信号
                    self.stats                        # 共享统计（需要改进）
                ),
                name=f"agent-{proc_id}",
                daemon=True
            )
            process.start()
            self.producer_processes.append(process)

        logger.info(f"Started {len(self.producer_processes)} agent processes")


def _agent_worker_func(agent_id, producer_infos, producer_properties,
                       work_assignment, stop_event, stats):
    """
    全局函数：模拟一个 agent 进程
    一个 agent 处理多个 producers（对应 Java 的一个线程）
    """
    import random
    import logging
    from benchmark.utils.uniform_rate_limiter import UniformRateLimiter
    from benchmark.worker.message_producer import MessageProducer
    from benchmark.worker.worker_stats import WorkerStats
    from benchmark.driver_kafka.kafka_benchmark_producer import KafkaBenchmarkProducer

    logger = logging.getLogger(__name__)
    logger.info(f"Agent {agent_id} started with {len(producer_infos)} producers")

    # ✓ 在进程内创建所有 producers
    producers = []
    for info in producer_infos:
        producer = KafkaBenchmarkProducer(
            info['topic'],
            producer_properties.copy()
        )
        producers.append({
            'producer': producer,
            'index': info['index']
        })

    # ✓ 创建独立的 stats 和 rate limiter
    local_stats = WorkerStats()  # 每个进程独立统计（后续需要汇总）

    # 计算此 agent 的速率（总速率 / agent 数量）
    total_rate = work_assignment.publish_rate
    # 注意：这里简化了，实际应该根据 producer 数量动态调整

    rate_limiter = UniformRateLimiter(total_rate / len(producer_infos))
    message_producer = MessageProducer(rate_limiter, local_stats)

    # 获取 payload
    payload = (work_assignment.payload_data[0]
               if work_assignment.payload_data
               else bytes(1024))

    try:
        # ✓ 主循环：像 Java 一样轮询所有 producers
        while not stop_event.is_set():
            for producer_info in producers:
                producer = producer_info['producer']
                index = producer_info['index']

                # 选择 key
                key = None
                if work_assignment.key_distributor_type:
                    if hasattr(work_assignment.key_distributor_type, 'name'):
                        if work_assignment.key_distributor_type.name == 'RANDOM':
                            key = str(random.randint(0, 1000000))
                        elif work_assignment.key_distributor_type.name == 'ROUND_ROBIN':
                            key = str(index)

                # ✓ 发送消息（带速率限制）
                try:
                    message_producer.send_message(producer, key, payload)
                except Exception as e:
                    logger.error(f"Agent {agent_id} send error: {e}")

    finally:
        # 清理
        for producer_info in producers:
            try:
                producer_info['producer'].close()
            except:
                pass

        logger.info(f"Agent {agent_id} stopped")
```

**这个实现的优势**:
1. ✅ **完全对应 Java 版本的设计**
2. ✅ 根据 CPU 核数创建进程（真正的多 agent 模拟）
3. ✅ 每个 agent 处理多个 producers（负载均衡）
4. ✅ 真正的并行（无 GIL 限制）
5. ✅ 更接近真实的分布式场景

---

### 方案 B: 混合架构（进程 + 线程）

```python
def _agent_worker_func(agent_id, producer_infos, producer_properties,
                       work_assignment, stop_event):
    """
    每个 agent 进程内再创建多个线程
    进一步提升并发性能
    """
    import threading
    from concurrent.futures import ThreadPoolExecutor

    logger.info(f"Agent {agent_id}: starting {len(producer_infos)} producer threads")

    # 在进程内创建 producers
    producers = [...]  # 同上

    # ✓ 使用线程池管理 producers（进程内）
    with ThreadPoolExecutor(max_workers=min(4, len(producers))) as executor:
        # 将 producers 分组
        producers_per_thread = len(producers) // 4 + 1

        for i in range(0, len(producers), producers_per_thread):
            thread_producers = producers[i:i + producers_per_thread]

            executor.submit(
                _producer_thread_func,
                agent_id,
                thread_producers,
                work_assignment,
                stop_event
            )

def _producer_thread_func(agent_id, producers, work_assignment, stop_event):
    """线程函数：处理一组 producers"""
    # ... 循环发送逻辑
```

**混合架构的优势**:
- ✅ 进程级并行（跨 CPU 核）
- ✅ 线程级并行（单核内）
- ✅ 最大化吞吐量

---

## 🐛 当前实现的问题

### 问题 1: 没有实现多 agent 模拟

**Java**:
```java
// 假设 8 核 CPU，100 个 producers
// 结果：8 个线程，每个线程处理 12-13 个 producers
```

**Python 当前**:
```python
# 100 个 producers
# 结果：100 个线程（❌ 错误！）
for producer in self.producers:  # 100 次循环
    thread = threading.Thread(...)
```

**应该是**:
```python
# 100 个 producers，8 核 CPU
# 结果：8 个进程，每个进程处理 12-13 个 producers
cpu_count = 8
for proc_id in range(cpu_count):  # 8 个进程
    producers_for_this_agent = ...  # 分配 12-13 个
    process = multiprocessing.Process(...)
```

---

### 问题 2: 统计数据无法汇总

**当前问题**:
```python
def _agent_worker_func(..., stats):  # ❌ WorkerStats 不能跨进程共享
    # 每个进程的统计数据独立
    # 主进程看不到子进程的数据
```

**解决方案 A: 使用 Queue 汇总**:
```python
def _agent_worker_func(agent_id, ..., stats_queue):
    local_stats = WorkerStats()

    # 定期发送统计到主进程
    last_report = time.time()

    while not stop_event.is_set():
        # ... 发送消息 ...

        # 每秒汇报一次
        if time.time() - last_report > 1.0:
            counters = local_stats.to_counters_stats()
            stats_queue.put({
                'agent_id': agent_id,
                'messages_sent': counters.messages_sent,
                'messages_received': counters.messages_received,
                'errors': counters.message_send_errors
            })
            last_report = time.time()

# 主进程收集统计
def _stats_collector_thread(stats_queue, global_stats):
    while True:
        try:
            agent_stats = stats_queue.get(timeout=1.0)
            # 汇总到全局统计
            global_stats.merge(agent_stats)
        except queue.Empty:
            continue
```

**解决方案 B: 使用 Manager 共享对象**:
```python
from multiprocessing import Manager

class LocalWorker:
    def __init__(self):
        self.manager = Manager()
        self.shared_counters = self.manager.dict({
            'messages_sent': 0,
            'messages_received': 0,
            'errors': 0
        })
        self.counter_lock = self.manager.Lock()

def _agent_worker_func(agent_id, ..., shared_counters, counter_lock):
    while not stop_event.is_set():
        # 发送消息
        success = send_message(...)

        # 更新共享计数器
        with counter_lock:
            if success:
                shared_counters['messages_sent'] += 1
            else:
                shared_counters['errors'] += 1
```

---

### 问题 3: Consumer 和 Producer 架构不一致

**当前状态**:
- Consumer: ✅ 使用 multiprocessing.Process
- Producer: ❌ 使用 threading.Thread

**问题**:
- 不符合"多 agent 模拟"的设计意图
- Consumer 是进程，Producer 是线程，概念混乱

**统一方案**:
```python
class LocalWorker:
    def create_consumers(self, assignment):
        """为每个 CPU 核创建一个 Consumer agent"""
        cpu_count = multiprocessing.cpu_count()

        # 将 consumers 分配到不同的 agent
        for agent_id in range(min(cpu_count, len(consumers))):
            consumers_for_agent = ...

            process = multiprocessing.Process(
                target=_consumer_agent_func,
                args=(agent_id, consumers_for_agent, ...)
            )
            process.start()

    def start_load(self, assignment):
        """为每个 CPU 核创建一个 Producer agent"""
        cpu_count = multiprocessing.cpu_count()

        for agent_id in range(cpu_count):
            producers_for_agent = ...

            process = multiprocessing.Process(
                target=_producer_agent_func,
                args=(agent_id, producers_for_agent, ...)
            )
            process.start()
```

---

## 📐 实现流程对比

### Java 版本流程

```
1. 获取 CPU 核数 (processors)
   └─> Runtime.getRuntime().availableProcessors()

2. 分配 Producers 到各个 "processor"
   └─> TreeMap<Integer, List<Producer>>
   └─> 轮询分配（Round-Robin）

3. 为每个 processor 创建一个线程
   └─> executor.submit(...)
   └─> 线程内循环处理多个 producers

4. 线程循环
   └─> while (!testCompleted)
       └─> forEach(producer -> send)

5. 统计收集
   └─> 所有线程共享 WorkerStats
   └─> 使用 synchronized/atomic 保证线程安全
```

### Python 当前版本流程（错误）

```
1. ❌ 没有获取 CPU 核数

2. ❌ 没有分配逻辑
   └─> 直接遍历所有 producers

3. ❌ 为每个 producer 创建一个线程
   └─> threading.Thread(...)
   └─> 100 个 producer = 100 个线程

4. 线程循环
   └─> while not stop_producing.is_set()
       └─> 只处理一个 producer

5. 统计收集
   └─> 所有线程共享 WorkerStats
   └─> 使用 multiprocessing.Lock
```

### Python 正确版本流程（推荐）

```
1. ✓ 获取 CPU 核数
   └─> multiprocessing.cpu_count()

2. ✓ 分配 Producers 到各个 agent
   └─> dict<int, List[ProducerInfo]>
   └─> 轮询分配（Round-Robin）

3. ✓ 为每个 agent 创建一个进程
   └─> multiprocessing.Process(...)
   └─> 进程内创建分配的 producers

4. ✓ 进程循环
   └─> while not stop_event.is_set()
       └─> for producer in producers:
           └─> send_message(...)

5. ✓ 统计收集（需要改进）
   方案 A: Queue 汇总
   └─> stats_queue.put(local_stats)
   └─> 主进程收集器线程

   方案 B: Manager 共享
   └─> manager.dict() 共享计数器
   └─> manager.Lock() 保护
```

---

## 🎯 推荐的完整实现

### 完整代码示例

```python
import multiprocessing
import threading
import queue
import time
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)


class LocalWorker(Worker):
    def __init__(self, stats_logger=None):
        self.benchmark_driver = None
        self.producers = []
        self.consumers = []
        self.stats = WorkerStats(stats_logger)

        # 多进程管理
        self.test_completed = multiprocessing.Event()
        self.producer_processes = []
        self.consumer_processes = []

        # 统计汇总
        self.stats_queue = multiprocessing.Queue()
        self.stats_collector_thread = None

    def start_load(self, producer_work_assignment):
        """启动负载 - 多 agent 模式"""
        cpu_count = multiprocessing.cpu_count()

        logger.info(f"Starting {cpu_count} producer agents (processes)")

        # 分配 producers 到各个 agent
        agent_assignments = self._assign_producers_to_agents(cpu_count)

        # 获取 producer 配置
        producer_properties = self.benchmark_driver.producer_properties.copy()

        # 启动统计收集器
        self._start_stats_collector()

        # 为每个 agent 启动一个进程
        for agent_id, producer_infos in agent_assignments.items():
            process = multiprocessing.Process(
                target=_producer_agent_worker,
                args=(
                    agent_id,
                    producer_infos,
                    producer_properties,
                    producer_work_assignment,
                    self.test_completed,
                    self.stats_queue
                ),
                name=f"producer-agent-{agent_id}",
                daemon=False  # 非 daemon，确保正常退出
            )
            process.start()
            self.producer_processes.append(process)

        logger.info(f"Started {len(self.producer_processes)} producer agent processes")

    def _assign_producers_to_agents(self, num_agents: int) -> Dict[int, List]:
        """将 producers 分配到各个 agent"""
        assignments = {i: [] for i in range(num_agents)}

        for idx, producer in enumerate(self.producers):
            agent_id = idx % num_agents  # 轮询分配
            assignments[agent_id].append({
                'topic': producer.topic,
                'index': idx
            })

        # 打印分配信息
        for agent_id, infos in assignments.items():
            logger.info(f"Agent {agent_id}: {len(infos)} producers")

        return assignments

    def _start_stats_collector(self):
        """启动统计收集器线程"""
        def collector_loop():
            while not self.test_completed.is_set():
                try:
                    stats_update = self.stats_queue.get(timeout=0.5)

                    # 汇总到全局统计
                    self.stats.messages_sent.add(stats_update['messages_sent'])
                    self.stats.message_send_errors.add(stats_update['errors'])

                except queue.Empty:
                    continue
                except Exception as e:
                    logger.error(f"Stats collector error: {e}")

        self.stats_collector_thread = threading.Thread(
            target=collector_loop,
            daemon=True,
            name="stats-collector"
        )
        self.stats_collector_thread.start()

    def stop_all(self):
        """停止所有 agents"""
        logger.info("Stopping all agents...")

        # 设置停止信号
        self.test_completed.set()

        # 等待所有进程退出
        for process in self.producer_processes:
            process.join(timeout=5.0)
            if process.is_alive():
                logger.warning(f"Force terminating {process.name}")
                process.terminate()
                process.join(timeout=1.0)

        self.producer_processes.clear()

        # 停止统计收集器
        if self.stats_collector_thread:
            self.stats_collector_thread.join(timeout=2.0)

        logger.info("All agents stopped")


def _producer_agent_worker(agent_id, producer_infos, producer_properties,
                           work_assignment, stop_event, stats_queue):
    """
    Producer Agent 工作函数（全局函数，支持 multiprocessing）

    模拟一个独立的 agent，处理分配给它的所有 producers
    """
    import random
    import time
    import logging
    from benchmark.utils.uniform_rate_limiter import UniformRateLimiter
    from benchmark.worker.message_producer import MessageProducer
    from benchmark.worker.worker_stats import WorkerStats
    from benchmark.driver_kafka.kafka_benchmark_producer import KafkaBenchmarkProducer

    # 配置日志
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(f"agent-{agent_id}")

    logger.info(f"Agent {agent_id} starting with {len(producer_infos)} producers")

    try:
        # 1. 在进程内创建 producers
        producers = []
        for info in producer_infos:
            producer = KafkaBenchmarkProducer(
                info['topic'],
                producer_properties.copy()
            )
            producers.append({
                'producer': producer,
                'index': info['index']
            })

        # 2. 创建本地统计和速率限制器
        local_stats = WorkerStats()

        # 计算此 agent 的总速率
        # 注意：总速率已经被分配了，这里是每个 producer 的速率
        total_rate = work_assignment.publish_rate
        num_total_producers = len(producer_infos)  # 简化：假设平均分配
        per_producer_rate = total_rate / num_total_producers if num_total_producers > 0 else 1.0

        rate_limiter = UniformRateLimiter(per_producer_rate)
        message_producer = MessageProducer(rate_limiter, local_stats)

        # 3. 准备 payload
        payload = (work_assignment.payload_data[0]
                   if work_assignment.payload_data
                   else bytes(1024))

        # 4. 主循环：轮询所有 producers 发送消息
        last_stats_report = time.time()
        messages_sent_since_last_report = 0

        while not stop_event.is_set():
            for producer_info in producers:
                if stop_event.is_set():
                    break

                producer = producer_info['producer']
                index = producer_info['index']

                # 选择 key
                key = None
                if work_assignment.key_distributor_type:
                    if hasattr(work_assignment.key_distributor_type, 'name'):
                        dist_type = work_assignment.key_distributor_type.name
                        if dist_type == 'RANDOM':
                            key = str(random.randint(0, 1000000))
                        elif dist_type == 'ROUND_ROBIN':
                            key = str(index)

                # 发送消息（带速率限制）
                try:
                    message_producer.send_message(producer, key, payload)
                    messages_sent_since_last_report += 1
                except Exception as e:
                    logger.error(f"Send error: {e}")

            # 5. 定期汇报统计（每秒）
            now = time.time()
            if now - last_stats_report >= 1.0:
                counters = local_stats.to_counters_stats()
                stats_queue.put({
                    'agent_id': agent_id,
                    'messages_sent': messages_sent_since_last_report,
                    'errors': counters.message_send_errors
                })

                logger.debug(f"Agent {agent_id}: sent {messages_sent_since_last_report} msgs/s")

                messages_sent_since_last_report = 0
                last_stats_report = now

    except Exception as e:
        logger.error(f"Agent {agent_id} fatal error: {e}", exc_info=True)

    finally:
        # 6. 清理资源
        logger.info(f"Agent {agent_id} shutting down...")
        for producer_info in producers:
            try:
                producer_info['producer'].close()
            except Exception as e:
                logger.error(f"Error closing producer: {e}")

        logger.info(f"Agent {agent_id} stopped")
```

---

## 📊 性能对比

### Java 多线程

| 场景 | 线程数 | 说明 |
|------|--------|------|
| 8 核 CPU, 100 producers | 8 | 每核一个线程 |
| 16 核 CPU, 100 producers | 16 | 每核一个线程 |
| 8 核 CPU, 10 producers | 8 | 部分线程空闲 |

**优点**:
- ✅ 简单，易于理解
- ✅ 共享内存，统计收集简单

**缺点**:
- ❌ 受 GIL 限制（Python 中）
- ❌ 不能真正并行（Python 中）

### Python 多进程（推荐）

| 场景 | 进程数 | 说明 |
|------|--------|------|
| 8 核 CPU, 100 producers | 8 | 每核一个进程，真正并行 |
| 16 核 CPU, 100 producers | 16 | 每核一个进程 |
| 8 核 CPU, 10 producers | 8 | 部分进程处理 1-2 个 |

**优点**:
- ✅ **真正并行**（无 GIL）
- ✅ **性能更高**
- ✅ 更接近真实分布式场景

**缺点**:
- ⚠️ 统计汇总复杂
- ⚠️ 需要序列化（pickle）

---

## ✅ 总结与建议

### 当前实现的根本问题

1. **❌ 没有实现多 agent 模拟**
   - 应该：CPU 核数个进程
   - 实际：producer 数量个线程

2. **❌ 没有负载均衡**
   - 应该：轮询分配 producers
   - 实际：每个 producer 一个线程

3. **❌ 架构不一致**
   - Consumer: 进程
   - Producer: 线程（错误）

### 推荐改进方案

1. **✅ 实现真正的多 agent 模拟**
   ```python
   cpu_count = multiprocessing.cpu_count()
   # 创建 cpu_count 个进程，每个代表一个 agent
   ```

2. **✅ 使用负载均衡分配**
   ```python
   # Round-Robin 分配 producers 到各个 agent
   agent_id = producer_index % cpu_count
   ```

3. **✅ 统一使用进程**
   ```python
   # Producer agents: multiprocessing.Process
   # Consumer agents: multiprocessing.Process
   ```

4. **✅ 实现统计汇总**
   ```python
   # 方案 A: Queue + collector thread
   # 方案 B: Manager.dict() + Lock
   ```

你的设计意图是**完全正确**的！只是当前实现偏离了这个目标。按照上述方案修改后，就能真正实现"多 agent 并行模拟"。

---

**文档生成时间**: 2025-10-03
**分析者**: Claude (Anthropic)
