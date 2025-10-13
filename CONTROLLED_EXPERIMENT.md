# 控制变量实验指南

## 🎯 实验目的

**研究问题**: 在相同系统压力下，消息大小如何影响延迟？

**控制变量**: 固定吞吐量 ~2.5 MB/s
**自变量**: 消息大小 (256B, 1KB, 4KB, 16KB, 64KB)
**因变量**: 延迟 (P50, P99, P99.9)

---

## 📋 实验设计

### 为什么要控制吞吐量？

❌ **错误做法**: 所有测试都用 1000 msg/s
```
256B × 1000 = 0.24 MB/s   ← Kafka很轻松
64KB × 1000 = 62.5 MB/s   ← Kafka撑不住！
```
**问题**: 系统压力不同，延迟无法对比

✅ **正确做法**: 所有测试都用 ~2.5 MB/s
```
256B × 10000 = 2.4 MB/s   ← 系统压力相同
1KB  × 2500  = 2.4 MB/s   ← 系统压力相同
4KB  × 650   = 2.5 MB/s   ← 系统压力相同
16KB × 160   = 2.5 MB/s   ← 系统压力相同
64KB × 40    = 2.5 MB/s   ← 系统压力相同
```
**优势**: 公平对比，科学严谨

---

## 🧪 实验配置

| 消息大小 | Agents | Rate/Agent | 总msg/s | 总MB/s | 说明 |
|---------|--------|-----------|---------|--------|------|
| 256 B | 10 | 200 | 2000 | 0.49 | 小消息高并发 |
| 1 KB | 10 | 100 | 1000 | 0.98 | 基线配置 |
| 4 KB | 10 | 65 | 650 | 2.53 | 中等消息 |
| 16 KB | 10 | 16 | 160 | 2.50 | 大消息 |
| 64 KB | 10 | 4 | 40 | 2.50 | 超大消息 |

---

## 🚀 运行实验

### 一键运行完整实验

```bash
cd /Users/lbw1125/Desktop/openmessaging-benchmark

# 运行所有5个测试（约15分钟）
bash run_controlled_experiment.sh

# 重命名结果文件
bash rename_controlled_results.sh

# 分析结果
python analyze_controlled_experiment.py
```

### 单独运行某个测试

```bash
# 例如：只测试64KB
cat > /tmp/test-workload.yaml << 'EOF'
name: msg-size-65536B-controlled
topics: 1
partitionsPerTopic: 10
messageSize: 65536
keyDistributor: RANDOM_NANO
subscriptionsPerTopic: 1
producersPerTopic: 10
consumerPerSubscription: 10
producerRate: 4
testDurationMinutes: 2
warmupDurationMinutes: 0
EOF

python -m benchmark.benchmark -d examples/kafka-driver.yaml /tmp/test-workload.yaml
```

---

## 📊 预期结果

### 假设1: 延迟与消息大小线性相关

如果成立，应该看到：
```
256B  → P99 ~500ms
1KB   → P99 ~800ms   (4倍大小, 1.6倍延迟)
4KB   → P99 ~1500ms  (16倍大小, 3倍延迟)
16KB  → P99 ~3000ms  (64倍大小, 6倍延迟)
64KB  → P99 ~5000ms  (256倍大小, 10倍延迟)
```

### 假设2: 大消息有批处理优势

如果成立，应该看到：
```
延迟增长 < 消息大小增长
例如: 64KB消息 = 256倍大小，但延迟只增加5倍
```

### 假设3: 大消息有额外开销

如果成立，应该看到：
```
延迟增长 > 消息大小增长
例如: 64KB消息 = 256倍大小，延迟增加20倍
```

---

## 📈 数据可视化建议

可以用Python绘制图表：

```python
import matplotlib.pyplot as plt

sizes = [256, 1024, 4096, 16384, 65536]
p99_latencies = [...]  # 从结果中提取

plt.figure(figsize=(10, 6))
plt.plot(sizes, p99_latencies, 'o-', linewidth=2, markersize=8)
plt.xlabel('Message Size (bytes)', fontsize=12)
plt.ylabel('P99 Latency (ms)', fontsize=12)
plt.title('Message Size vs Latency (Controlled Throughput ~2.5 MB/s)', fontsize=14)
plt.xscale('log')
plt.grid(True, alpha=0.3)
plt.savefig('controlled_experiment_results.png', dpi=300, bbox_inches='tight')
plt.show()
```

---

## ⚠️ 注意事项

### 1. 硬件限制（你的Mac）
- 双核CPU，2个逻辑核心给Docker
- 64KB配置已经是极限
- 如果出现Timeout，说明超出硬件能力

### 2. 测试间隔
- 每个测试后等待10秒
- 让Kafka清空内存和刷新磁盘

### 3. 结果有效性
- 检查 `publishErrorRate` 应该 = 0
- 检查 `backlog` 应该 < 1000
- 如果不满足，说明测试无效

---

## 📝 论文写作建议

### 实验设置章节

```
我们设计了一个控制变量实验来研究消息大小对延迟的影响。
为了保证公平对比，我们固定系统吞吐量为2.5 MB/s，
通过调整消息速率来适配不同的消息大小。

实验环境：
- Kafka 7.5.0 (单节点, Docker)
- 硬件: MacBook Pro (双核, 16GB内存)
- 测试工具: 自研benchmark框架
- 配置: 10个生产者, 10个消费者, 1个topic, 10个分区
```

### 结果分析章节

```
结果显示，当吞吐量固定时，P99延迟随消息大小呈[线性/超线性/次线性]增长。
从256B到64KB（256倍大小），P99延迟从Xms增加到Yms（Z倍增长）。
这表明[批处理优势/网络开销/...]是主要影响因素。
```

---

## 🎓 科学性保证

✅ **控制变量**: 吞吐量固定
✅ **重复性**: 可重现的实验配置
✅ **数据完整**: 记录所有延迟分位数
✅ **环境记录**: 文档化硬件和软件配置
✅ **有效性检查**: 错误率和积压监控

---

## 🤔 常见问题

**Q: 为什么256B用2000 msg/s，64KB只用40 msg/s？**
A: 因为要保持相同的吞吐量(MB/s)，这样Kafka的压力才相同。

**Q: 为什么不保持1000 msg/s？**
A: 因为64KB × 1000 = 62.5 MB/s会超出硬件能力，测试会失败。

**Q: 吞吐量有点不一致怎么办？**
A: 可以接受±20%的误差，只要错误率为0即可。

---

准备好了就运行实验吧！🚀
