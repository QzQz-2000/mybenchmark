# 重构总结：Java OMB 风格持续运行模式

## 已完成的修改

### 1. 数据模型 (benchmark/core/worker.py)
- **ProducerTask**: 移除 `num_messages`，添加注释说明持续运行模式
- **ConsumerTask**: 移除 `test_duration_seconds`，添加注释说明持续运行模式

### 2. Producer 进程逻辑 (benchmark/worker/process_executor.py)
- **_producer_process_main()**: 添加 `stop_event` 参数
- **_run_producer_async()**:
  - 添加 `stop_event` 参数
  - 改为 `while not stop_event.is_set()` 循环
  - 移除固定消息数量限制
  - 持续按速率发送消息直到收到停止信号

### 3. Consumer 进程逻辑 (benchmark/worker/process_executor.py)
- **_consumer_process_main()**: 添加 `stop_event` 参数
- **_run_consumer_async()**:
  - 添加 `stop_event` 参数
  - 改为 `while not stop_event.is_set()` 循环
  - 移除固定测试时长
  - 持续消费消息直到收到停止信号

### 4. ProcessExecutor (benchmark/worker/process_executor.py)
- 添加 `self._stop_event = mp.Event()` 共享停止信号
- **execute_producer_tasks()**: 传递 `stop_event` 给 Producer 进程
- **execute_consumer_tasks()**: 传递 `stop_event` 给 Consumer 进程
- 新增 **stop_all()** 方法: 设置 stop_event 触发所有进程停止
- 新增 **wait_for_completion()** 方法: 等待所有进程完成并收集结果

### 5. Task 生成逻辑 (benchmark/core/coordinator.py)
- **_generate_producer_tasks()**:
  - 移除 `num_messages` 计算
  - 只设置 `rate_limit`，持续模式
- **_generate_consumer_tasks()**:
  - 移除 `test_duration_seconds` 参数
  - Consumer 将持续运行

## 还需要完成的工作

### 6. Coordinator 主流程重构
需要修改 `_run_test_phase()` 方法：

```python
async def _run_test_phase(self, ...):
    # 1. 启动 Consumers (不等待完成)
    consumer_futures = []
    for worker_url in self.config.workers:
        future = self._start_consumers_non_blocking(worker_url, tasks)
        consumer_futures.append(future)

    # 2. 等待 consumers 订阅完成
    await asyncio.sleep(5)

    # 3. 启动 Producers (不等待完成)
    producer_futures = []
    for worker_url in self.config.workers:
        future = self._start_producers_non_blocking(worker_url, tasks)
        producer_futures.append(future)

    # 4. 等待测试时长
    test_duration = workload_config.test_duration_minutes * 60
    self.logger.info(f"⏱️  Running test for {test_duration} seconds...")
    await asyncio.sleep(test_duration)

    # 5. 触发停止信号 (Java OMB: testCompleted = true)
    self.logger.info("🛑 Triggering stop signal for all workers...")
    await self._stop_all_workers()

    # 6. 等待所有进程完成
    all_results = await self._wait_all_workers_completion()

    return all_results
```

### 7. Worker API 更新
需要在 `kafka_worker_multiprocess.py` 中添加新的 API 端点：
- `POST /stop-all`: 触发 `executor.stop_all()`
- `POST /wait-completion`: 调用 `executor.wait_for_completion()`

或者使用单一 API：
- `POST /producer/start-continuous`: 启动但不等待完成
- `POST /consumer/start-continuous`: 启动但不等待完成
- `POST /stop-and-collect`: 停止所有进程并收集结果

## 核心设计原理

### Java OMB 模型
```java
// Start all producers and consumers
startProducersAndConsumers();

// Run for test duration
Thread.sleep(testDurationMinutes * 60 * 1000);

// Stop all
testCompleted = true;  // Signal to all threads
stopAll();
```

### Python 版本实现
```python
# multiprocessing.Event 作为 stop 信号
stop_event = mp.Event()

# Producer/Consumer 进程
while not stop_event.is_set():
    # 持续工作...

# Coordinator 控制
await asyncio.sleep(test_duration)
stop_event.set()  # 触发停止
```

## 优势
1. **真正的 Java OMB 等价实现**：行为完全一致
2. **无时序问题**：Producer/Consumer 同时运行，Consumer 不会错过消息
3. **准确的测试时长**：由 Coordinator 统一控制
4. **优雅停止**：所有进程收到信号后完成当前操作再退出

