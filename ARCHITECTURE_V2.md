# LocalWorker V2 架构文档

## 概述

LocalWorker V2实现了完全独立的Producer和Consumer进程架构，每个Producer和每个Consumer都运行在独立的进程中，真正实现了数字孪生场景下的Agent隔离。

## V1 vs V2 架构对比

### V1 架构（旧版本 - 已废弃）

```
┌─────────────────────────────────────────────────────────────────┐
│                     LocalWorker 主进程                           │
│                                                                  │
│  ┌────────────────┐                                             │
│  │ Stats Collector│                                             │
│  │    Thread      │                                             │
│  └────────────────┘                                             │
│         ↑                                                        │
│         │ stats_queue (multiprocessing.Queue)                   │
│         │                                                        │
│  ┌──────┴──────────────────────────────────────────────┐       │
│  │                                                      │       │
│  │  ┌──────────────────┐                               │       │
│  │  │ All Consumers    │  ← Python GIL限制！           │       │
│  │  │ (在主进程中)      │     无法真正并行              │       │
│  │  │ - Consumer 0     │                               │       │
│  │  │ - Consumer 1     │                               │       │
│  │  │ - Consumer N     │                               │       │
│  │  └──────────────────┘                               │       │
│  │                                                      │       │
│  └──────────────────────────────────────────────────────┘       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

          ↓ spawns independent processes

┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ Producer Agent 0 │  │ Producer Agent 1 │  │ Producer Agent N │
│                  │  │                  │  │                  │
│ ┌──────────────┐ │  │ ┌──────────────┐ │  │ ┌──────────────┐ │
│ │Kafka Producer│ │  │ │Kafka Producer│ │  │ │Kafka Producer│ │
│ │  (独立进程)   │ │  │ │  (独立进程)   │ │  │ │  (独立进程)   │ │
│ └──────────────┘ │  │ └──────────────┘ │  │ └──────────────┘ │
│                  │  │                  │  │                  │
│  Rate Limiter   │  │  Rate Limiter   │  │  Rate Limiter   │
│  Local Stats    │  │  Local Stats    │  │  Local Stats    │
└──────────────────┘  └──────────────────┘  └──────────────────┘
         ↓                     ↓                     ↓
    stats_queue          stats_queue          stats_queue
```

**V1 问题：**
- ❌ 所有Consumer在主进程中，受Python GIL限制
- ❌ 无法真正并行消费消息
- ❌ 多个Consumer之间会相互争抢GIL
- ❌ 不符合数字孪生场景需求（每个Agent应该独立）

---

### V2 架构（新版本 - 推荐）

```
┌─────────────────────────────────────────────────────────────────┐
│                     LocalWorker 主进程                           │
│                                                                  │
│  ┌────────────────┐                                             │
│  │ Stats Collector│                                             │
│  │    Thread      │                                             │
│  └────────────────┘                                             │
│         ↑                                                        │
│         │ stats_queue (multiprocessing.Queue)                   │
│         │                                                        │
│  ┌──────┴──────────────────────────────────────────────┐       │
│  │          仅存储元数据                                │       │
│  │  - producer_metadata[]                              │       │
│  │  - consumer_metadata[]                              │       │
│  │                                                      │       │
│  │  不再创建实际的Consumer对象！                        │       │
│  └──────────────────────────────────────────────────────┘       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

          ↓ spawns ALL agents as independent processes

┌───────────────────────────────────────────────────────────────────┐
│                    Producer Agent Processes                        │
└───────────────────────────────────────────────────────────────────┘

┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ Producer Agent 0 │  │ Producer Agent 1 │  │ Producer Agent N │
│                  │  │                  │  │                  │
│ ┌──────────────┐ │  │ ┌──────────────┐ │  │ ┌──────────────┐ │
│ │Kafka Producer│ │  │ │Kafka Producer│ │  │ │Kafka Producer│ │
│ │  (独立进程)   │ │  │ │  (独立进程)   │ │  │ │  (独立进程)   │ │
│ └──────────────┘ │  │ └──────────────┘ │  │ └──────────────┘ │
│                  │  │                  │  │                  │
│  Rate Limiter   │  │  Rate Limiter   │  │  Rate Limiter   │
│  Local Stats    │  │  Local Stats    │  │  Local Stats    │
│  Pub Latency    │  │  Pub Latency    │  │  Pub Latency    │
└──────────────────┘  └──────────────────┘  └──────────────────┘
         ↓                     ↓                     ↓
    stats_queue          stats_queue          stats_queue

┌───────────────────────────────────────────────────────────────────┐
│                    Consumer Agent Processes (V2 新增!)             │
└───────────────────────────────────────────────────────────────────┘

┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ Consumer Agent 0 │  │ Consumer Agent 1 │  │ Consumer Agent N │
│                  │  │                  │  │                  │
│ ┌──────────────┐ │  │ ┌──────────────┐ │  │ ┌──────────────┐ │
│ │Kafka Consumer│ │  │ │Kafka Consumer│ │  │ │Kafka Consumer│ │
│ │  (独立进程)   │ │  │ │  (独立进程)   │ │  │ │  (独立进程)   │ │
│ │  group.id    │ │  │ │  group.id    │ │  │ │  group.id    │ │
│ └──────────────┘ │  │ └──────────────┘ │  │ └──────────────┘ │
│                  │  │                  │  │                  │
│  Local Stats    │  │  Local Stats    │  │  Local Stats    │
│  E2E Latency    │  │  E2E Latency    │  │  E2E Latency    │
│  Poll Loop      │  │  Poll Loop      │  │  Poll Loop      │
└──────────────────┘  └──────────────────┘  └──────────────────┘
         ↓                     ↓                     ↓
    stats_queue          stats_queue          stats_queue
```

**V2 优势：**
- ✅ 每个Producer独立进程
- ✅ 每个Consumer独立进程（新增）
- ✅ 完全绕过Python GIL限制
- ✅ 真正的并行消费
- ✅ 符合数字孪生场景（每个Agent完全独立）
- ✅ Kafka Consumer Group自动分配partition

---

## 关键实现细节

### 1. 进程架构

**V1 (旧版):**
```
主进程 = LocalWorker + All Consumers
子进程 = Producer Agent 0
子进程 = Producer Agent 1
...
```

**V2 (新版):**
```
主进程 = LocalWorker (仅orchestrator)
子进程 = Producer Agent 0
子进程 = Producer Agent 1
...
子进程 = Consumer Agent 0  ← 新增!
子进程 = Consumer Agent 1  ← 新增!
...
```

### 2. 统计数据收集

所有Agent进程通过`multiprocessing.Queue`向主进程发送统计数据：

**Producer Agent发送:**
```python
stats_queue.put({
    'agent_id': 0,
    'type': 'producer',  # 类型标识
    'messages_sent': 100,
    'bytes_sent': 102400,
    'pub_latency_samples': [1.2, 1.5, 2.1, ...],
    'pub_delay_samples': [0.5, 0.3, ...],
    'epoch': 1
})
```

**Consumer Agent发送 (V2新增):**
```python
stats_queue.put({
    'agent_id': 0,
    'type': 'consumer',  # 类型标识
    'messages_received': 100,
    'bytes_received': 102400,
    'e2e_latency_samples': [5.2, 6.1, 7.3, ...],  # 端到端延迟
    'epoch': 1
})
```

### 3. Stats Collector线程处理

```python
def _start_stats_collector(self):
    while self.stats_collector_running:
        stats_dict = self.stats_queue.get(timeout=0.5)
        agent_type = stats_dict.get('type', 'producer')

        if agent_type == 'producer':
            # 处理Producer统计
            self.stats.messages_sent.add(stats_dict['messages_sent'])
            # 记录pub latency样本
            for sample in stats_dict['pub_latency_samples']:
                self.stats.publish_latency_recorder.record_value(sample)

        elif agent_type == 'consumer':  # V2新增
            # 处理Consumer统计
            self.stats.messages_received.add(stats_dict['messages_received'])
            # 记录E2E latency样本
            for sample in stats_dict['e2e_latency_samples']:
                self.stats.end_to_end_latency_recorder.record_value(sample)
```

### 4. Consumer Group机制

V2架构利用Kafka Consumer Group实现分区自动分配：

```
Topic: test-topic-0 (10 partitions)
Consumer Group: subscription-0

假设有5个Consumer Agent:
┌─────────────────────┬──────────────────────────┐
│  Consumer Agent     │  自动分配的Partitions      │
├─────────────────────┼──────────────────────────┤
│  Consumer Agent 0   │  Partition 0, 1           │
│  Consumer Agent 1   │  Partition 2, 3           │
│  Consumer Agent 2   │  Partition 4, 5           │
│  Consumer Agent 3   │  Partition 6, 7           │
│  Consumer Agent 4   │  Partition 8, 9           │
└─────────────────────┴──────────────────────────┘
```

**关键规则：**
- 同一个Consumer Group内，每个partition只分配给1个Consumer
- 如果Consumer数量 > Partition数量，多余的Consumer会空闲
- 如果Consumer数量 < Partition数量，部分Consumer会处理多个Partition

### 5. 端到端延迟计算

**V1 (旧版):**
- Consumer在主进程中通过callback计算E2E延迟
- 主进程直接调用 `self.stats.record_message_received()`

**V2 (新版):**
- Consumer在独立进程中poll消息时计算E2E延迟
- 本地累积样本，每秒发送到主进程

```python
# isolated_consumer_agent.py
msg = consumer.poll(timeout=1.0)
if msg and not msg.error():
    # 计算E2E延迟
    publish_timestamp_ms = msg.timestamp()[1]  # Producer发送时间
    receive_timestamp_ms = int(time.time() * 1000)  # 当前接收时间
    e2e_latency_ms = receive_timestamp_ms - publish_timestamp_ms

    # 记录到本地样本数组
    local_stats.e2e_latency_samples.append(e2e_latency_ms)

# 每秒发送一次
if now - last_stats_report >= 1.0:
    stats_queue.put({
        'agent_id': agent_id,
        'type': 'consumer',
        'e2e_latency_samples': local_stats.e2e_latency_samples[:]
    })
    local_stats.e2e_latency_samples.clear()
```

---

## 关键文件

### 新增文件

1. **`benchmark/worker/isolated_consumer_agent.py`**
   - Consumer Agent独立进程实现
   - 在进程内创建Kafka Consumer
   - Poll消息并计算E2E延迟
   - 定期发送统计到主进程

### 修改文件

1. **`benchmark/worker/local_worker.py`**
   - 添加 `consumer_metadata` 列表
   - 修改 `create_consumers()`: 只存储元数据，不创建实际对象
   - 修改 `start_load()`: 同时spawn Producer和Consumer Agent进程
   - 修改 `_start_stats_collector()`: 区分处理producer和consumer统计
   - 更新 `pause_consumers()`, `resume_consumers()`: 添加V2架构警告
   - 更新 `stop_all()`: 清理consumer_metadata
   - 更新 `message_received()`: 标记为V1兼容

---

## 进程间通信机制

### multiprocessing.Queue (stats_queue)

**用途:** Agent进程 → 主进程统计数据传输

**数据流:**
```
Producer Agent 0 ──┐
Producer Agent 1 ──┤
...                 ├──> stats_queue ──> Stats Collector Thread
Consumer Agent 0 ──┤
Consumer Agent 1 ──┤
...                ┘
```

**配置:**
- `maxsize=10000`: 队列最大容量
- 使用 `timeout=0.1` 避免队列满时阻塞
- 队列满时丢弃统计（避免Agent进程hang）

### multiprocessing.Event (stop_agents)

**用途:** 主进程 → Agent进程停止信号

**实现:**
```python
# 主进程
self.stop_agents.set()  # 通知所有Agent停止

# Agent进程
while not stop_event.is_set():
    # 工作循环
    ...
```

### multiprocessing.Value (shared_publish_rate)

**用途:** 主进程 → Producer Agent动态速率调整

**实现:**
```python
# 主进程
self.shared_publish_rate.value = 2000  # 调整为2000 msg/s

# Producer Agent
if now - last_rate_check > 1.0:
    new_rate = self.shared_publish_rate.value
    if new_rate != current_rate:
        rate_limiter.set_rate(new_rate)
```

### multiprocessing.Value (reset_stats_flag)

**用途:** 主进程 → Agent进程统计重置（epoch机制）

**实现:**
```python
# 主进程
self.reset_stats_flag.value += 1  # 进入新epoch

# Agent进程
if reset_flag.value > current_epoch:
    current_epoch = reset_flag.value
    local_stats.clear()  # 重置本地统计
```

### multiprocessing.Queue (agent_ready_queue)

**用途:** Agent进程 → 主进程就绪/错误信号

**实现:**
```python
# Agent进程
ready_queue.put({'agent_id': 0, 'status': 'ready', 'type': 'consumer'})

# 主进程
msg = self.agent_ready_queue.get(timeout=remaining)
if msg['status'] == 'ready':
    ready_count += 1
elif msg['status'] == 'error':
    errors.append(msg['error'])
```

---

## 性能优势

### GIL限制对比

**V1 架构 (有GIL限制):**
```
10个Consumer在主进程 = 实际只有1个CPU核心工作
→ 即使有10个partition，也无法并行消费
→ 延迟高，吞吐量低
```

**V2 架构 (无GIL限制):**
```
10个Consumer独立进程 = 10个CPU核心同时工作
→ 真正并行消费10个partition
→ 延迟低，吞吐量高
```

### 实验结果预期

**Over-Provisioning测试 (10 partitions):**
- V1: 1个Consumer和10个Consumer性能接近（都受GIL限制）
- V2: 1→10个Consumer性能持续提升，10个时达到最优

**Under-Provisioning测试 (20 partitions):**
- V1: Consumer数量增加时提升有限（GIL瓶颈）
- V2: Consumer数量增加时持续提升，直到20个达到最优

---

## 使用指南

### 运行V2架构

V2架构是默认且唯一的架构，直接运行benchmark即可：

```bash
python -m benchmark.benchmark \
    -d examples/kafka-driver.yaml \
    -o results.json \
    workload.yaml
```

### 配置示例

**workload.yaml**
```yaml
name: test-v2-architecture

topics: 1
partitionsPerTopic: 10
messageSize: 1024

subscriptionsPerTopic: 1
producersPerTopic: 1
consumerPerSubscription: 10  # 10个独立Consumer进程

producerRate: 1000
testDurationMinutes: 2
warmupDurationMinutes: 1
```

### 监控Agent进程

```bash
# 查看所有agent进程
ps aux | grep "agent"

# 应该看到:
# producer-agent-0
# consumer-agent-0
# consumer-agent-1
# ...
```

---

## 故障排查

### Consumer Agent启动失败

**症状:**
```
Warning: Only 5/10 Agents reported ready (timeout or crash)
```

**排查:**
1. 检查Kafka连接配置
2. 检查topic和subscription是否存在
3. 查看日志中的ERROR信息
4. 检查Consumer Group是否有冲突

### 统计数据为0

**症状:**
```
Consumer rate: 0.0 msg/s
E2E latency: 0.0 ms
```

**可能原因:**
1. Consumer Agent未成功启动
2. Producer和Consumer订阅的topic不匹配
3. stats_queue队列满，统计数据被丢弃

**解决:**
1. 检查 `ready_count` 是否等于预期Agent数量
2. 检查日志中的 "Consumer Agent X is ready" 消息
3. 增大 `stats_queue` 的 `maxsize`

### 部分Consumer空闲

**症状:**
```
20个Consumer，但只有10个在工作
```

**原因:**
- Partition数量 < Consumer数量
- 根据Kafka规则，多余的Consumer会空闲

**解决:**
- 增加partition数量，或减少consumer数量

---

## 总结

V2架构实现了完全独立的Producer和Consumer进程，完美解决了Python GIL限制问题，真正实现了数字孪生场景下的Agent隔离架构。

**关键改进:**
1. ✅ 每个Consumer独立进程
2. ✅ 真正的并行消费
3. ✅ 利用Kafka Consumer Group自动分区分配
4. ✅ 进程内计算E2E延迟，准确性更高
5. ✅ 完全符合数字孪生场景需求

**下一步:**
- 运行实验脚本验证性能提升
- 对比V1和V2的性能差异
- 测试不同consumer/partition组合
