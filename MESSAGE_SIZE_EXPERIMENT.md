# 消息大小性能实验指南

## 📋 实验目的

研究不同消息大小对Kafka性能的影响：
- 吞吐量 (msg/s 和 MB/s)
- 延迟 (P50, P99, P99.9)
- 稳定性 (错误率、积压)

## 🧪 实验设计

### 测试配置
- **Producer数量**: 10个Agent
- **Producer速率**: 每个100 msg/s (总1000 msg/s)
- **测试时长**: 每个配置2分钟
- **消息大小**: 256B, 1KB, 4KB, 16KB, 64KB

### 预期结果

| 消息大小 | 预期吞吐量 (MB/s) | 预期现象 |
|---------|------------------|---------|
| 256 B | ~0.24 MB/s | 最低延迟，高消息吞吐 |
| 1 KB | ~0.98 MB/s | 基线配置 |
| 4 KB | ~3.9 MB/s | 批处理效率提升 |
| 16 KB | ~15.6 MB/s | 可能出现延迟增加 |
| 64 KB | ~62.5 MB/s | 可能接近Kafka单条消息限制 |

## 🚀 运行实验

### 方法1: 自动化运行（推荐）

```bash
cd /Users/lbw1125/Desktop/openmessaging-benchmark

# 运行完整实验（约15分钟）
bash run_message_size_experiment.sh

# 分析结果
python analyze_message_size_results.py
```

### 方法2: 手动逐个测试

```bash
# 测试 256B
cat > /tmp/test-workload.yaml << 'EOF'
name: msg-size-256B
topics: 1
partitionsPerTopic: 10
messageSize: 256
keyDistributor: RANDOM_NANO
subscriptionsPerTopic: 1
producersPerTopic: 10
consumerPerSubscription: 10
producerRate: 100
testDurationMinutes: 2
warmupDurationMinutes: 0
EOF

python -m benchmark.benchmark -d examples/kafka-driver.yaml /tmp/test-workload.yaml

# 对其他大小重复以上步骤，修改messageSize值为: 1024, 4096, 16384, 65536
```

## 📊 查看结果

### 结果文件位置

- **JSON结果**: `msg-size-*-Kafka-*.json`
- **日志文件**: `results/log-*B-*.txt`

### 关键指标

```bash
# 快速查看所有结果的P99延迟
for f in msg-size-*-Kafka-*.json; do
  echo -n "$f: "
  grep -o '"aggregatedPublishLatency99pct": [0-9.]*' "$f"
done

# 查看吞吐量
for f in msg-size-*-Kafka-*.json; do
  echo "$f:"
  grep -o '"publishRate": \[[^]]*\]' "$f"
  echo ""
done
```

## 🔍 预期发现

### 1. 吞吐量趋势
- **字节吞吐量 (MB/s)**: 随消息大小线性增长
- **消息吞吐量 (msg/s)**: 保持在1000 msg/s左右（如果稳定）

### 2. 延迟趋势
- **小消息 (256B-1KB)**: 延迟最低，~100-200ms
- **中等消息 (4KB)**: 延迟略增，批处理开始显现
- **大消息 (16KB-64KB)**: 延迟可能显著增加，受限于：
  - 网络传输时间
  - Kafka批处理策略
  - 磁盘IO速度

### 3. 性能瓶颈
可能出现的瓶颈点：
- **网络带宽**: 64KB × 1000 msg/s = 62.5 MB/s
- **磁盘IO**: 取决于本地磁盘性能
- **Kafka配置**: `batch.size`、`linger.ms` 等参数影响

## 📈 可视化建议

可以使用以下工具绘制图表：

```python
# 使用matplotlib绘制性能曲线
import matplotlib.pyplot as plt
import json

# 读取所有结果
sizes = [256, 1024, 4096, 16384, 65536]
latencies = []
throughputs = []

for size in sizes:
    # 读取对应的JSON文件
    # ... 提取数据 ...
    pass

# 绘制双Y轴图
fig, ax1 = plt.subplots()
ax1.plot(sizes, latencies, 'b-o', label='P99 Latency')
ax1.set_xlabel('Message Size (bytes)')
ax1.set_ylabel('Latency (ms)', color='b')

ax2 = ax1.twinx()
ax2.plot(sizes, throughputs, 'r-s', label='Throughput')
ax2.set_ylabel('Throughput (MB/s)', color='r')

plt.title('Message Size vs Performance')
plt.savefig('message_size_performance.png')
```

## ⚠️ 注意事项

1. **Kafka消息大小限制**
   - 默认最大消息: 1MB (`message.max.bytes`)
   - 如果测试更大消息，需要调整Kafka配置

2. **测试间隔**
   - 每个测试后等待10秒，让Kafka恢复
   - 避免前一个测试的积压影响下一个

3. **结果解读**
   - 关注P99/P99.9而非平均值（尾延迟更重要）
   - 检查错误率和积压，确保测试有效
   - 对比MB/s吞吐量，而非msg/s

## 🎯 实验结论模板

实验完成后，可以总结：

1. **最佳吞吐量配置**: XXX KB 消息，达到 XX MB/s
2. **最低延迟配置**: XXX B 消息，P99延迟 XX ms
3. **推荐生产配置**: XXX KB (平衡吞吐量和延迟)
4. **性能瓶颈**: 在 XX KB 时开始出现 [网络/磁盘/CPU] 瓶颈

## 📚 参考资料

- [Kafka Producer性能调优](https://kafka.apache.org/documentation/#producerconfigs)
- [消息批处理原理](https://kafka.apache.org/documentation/#producerconfigs_batch.size)
- [HdrHistogram用法](http://hdrhistogram.github.io/HdrHistogram/)
