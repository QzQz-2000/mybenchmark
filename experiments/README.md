# Consumer Scaling Experiments

This directory contains a comprehensive suite of experiments to understand Kafka consumer scaling behavior.

## 🎯 Overview

These experiments systematically test how consumer count affects latency and throughput under different conditions.

## 📋 Experiments

### Experiment 1: Over-Provisioning Test
**Script**: `exp1_over_provisioning.sh`

**Question**: What happens when you have more consumers than partitions?

**Setup**:
- Partitions: 10 (fixed)
- Consumers: 1, 5, 10, 15, 20
- Producer Rate: 1,000 msg/s
- Duration: 1 min per test (fast mode, no warmup)

**Expected Result**:
- Latency improves from 1→10 consumers
- No improvement from 10→20 consumers
- Extra 10 consumers remain idle

**Why It Matters**:
- Proves that over-provisioning wastes resources
- Validates the "1 consumer per partition" rule

---

### Experiment 2: Under-Provisioning Test
**Script**: `exp2_under_provisioning.sh`

**Question**: What happens when you have fewer consumers than partitions?

**Setup**:
- Partitions: 20 (fixed)
- Consumers: 5, 10, 15, 20
- Producer Rate: 1,000 msg/s
- Duration: 1 min per test (fast mode, no warmup)

**Expected Result**:
- Latency continuously improves as consumer count increases
- At 5 consumers: each handles 4 partitions (high latency)
- At 20 consumers: each handles 1 partition (optimal latency)

**Why It Matters**:
- Shows the cost of under-provisioning
- Demonstrates diminishing returns as you approach optimal count

---

### Experiment 3: High Load Test
**Script**: `exp3_high_load.sh`

**Question**: How does consumer scaling perform under heavy load?

**Setup**:
- Partitions: 10 (fixed)
- Consumers: 1, 5, 10
- Producer Rate: 10,000 msg/s (10x baseline)
- Duration: 1 min per test (fast mode, no warmup)

**Expected Result**:
- 1 consumer: may not keep up (backlog grows)
- 10 consumers: can handle the load with good latency
- Parallelization benefits are MORE pronounced under load

**Why It Matters**:
- Validates capacity planning under real-world load
- Shows when consumer parallelization becomes critical

---

## 🚀 Running the Experiments

### Run Individual Experiment

```bash
cd /Users/lbw1125/Desktop/openmessaging-benchmark

# Make scripts executable
chmod +x experiments/*.sh

# Run a specific experiment
bash experiments/exp1_over_provisioning.sh
bash experiments/exp2_under_provisioning.sh
bash experiments/exp3_high_load.sh
```

### Run All Experiments

```bash
# Run the complete suite (takes ~60 minutes)
bash experiments/run_all_experiments.sh
```

This will:
1. Run all 3 experiments sequentially
2. Copy results to a master directory
3. Generate a comprehensive comparison report

---

## 📊 Understanding the Results

### Key Metrics

| Metric | What It Measures | Good Value |
|--------|------------------|------------|
| **E2E Avg** | Average end-to-end latency | Lower is better |
| **E2E P50** | Median latency | Consistent = good |
| **E2E P99** | 99th percentile latency | Low = no tail latency |
| **Pub Rate** | Producer throughput | Should match target rate |
| **Cons Rate** | Consumer throughput | Should ≥ producer rate |

### What to Look For

✅ **Good Signs**:
- Consumer rate ≥ producer rate (no backlog)
- E2E latency decreases with more consumers (up to partition count)
- P99 latency is close to P50 (consistent performance)

⚠️ **Warning Signs**:
- Consumer rate < producer rate (falling behind)
- Latency doesn't improve with more consumers (already at optimal)
- High P99 compared to P50 (tail latency spikes)

---

## 🎓 Key Learnings

### The Golden Rule
```
Optimal Consumer Count = Partition Count
```

### Why?
- **Each partition** can only be consumed by **one consumer** in a group
- Having **more consumers than partitions** = idle consumers
- Having **fewer consumers than partitions** = each consumer handles multiple partitions serially

### Real-World Implications

1. **Capacity Planning**:
   - Want to scale consumers? Scale partitions too!
   - Set partitions at creation time (hard to change later)

2. **Latency Optimization**:
   - If latency is high, check consumer:partition ratio
   - Target 1:1 ratio for best latency

3. **Cost Optimization**:
   - Don't over-provision consumers (wastes resources)
   - But under-provisioning hurts latency significantly

---

## 📈 Sample Results

### Baseline (1,000 msg/s, 10 partitions)

| Consumers | E2E Avg | E2E P99 | Improvement |
|-----------|---------|---------|-------------|
| 1         | 123 ms  | 983 ms  | baseline    |
| 5         | 88 ms   | 676 ms  | 28.6% ↓     |
| 10        | 71 ms   | 670 ms  | 41.9% ↓     |

### High Load (10,000 msg/s, 10 partitions)

| Consumers | E2E Avg | Can Keep Up? |
|-----------|---------|--------------|
| 1         | 500+ ms | ⚠️ No        |
| 5         | 200 ms  | ✅ Yes       |
| 10        | 100 ms  | ✅ Yes       |

---

## 🔧 Prerequisites

- Kafka running on `localhost:9092`
- Python 3.8+ with benchmark dependencies installed
- ~20 minutes for full suite (12 tests × ~1.5 min each, fast mode)

---

## 🐛 Troubleshooting

### "Topic already exists" error
```bash
# Clear Python cache and wait for Kafka to delete topics
find . -type d -name "__pycache__" -exec rm -rf {} +
sleep 30
```

### High latency across all tests
- Check Kafka broker health
- Verify no other processes consuming resources
- Reduce `producerRate` if system is saturated

### Test hangs during warmup
- Fixed in workload_generator.py (warmup bug)
- Ensure you're using the patched version

---

## 📚 Further Reading

- [Kafka Consumer Groups](https://kafka.apache.org/documentation/#consumergroups)
- [Partition Assignment Strategy](https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy)
- [OpenMessaging Benchmark](https://openmessaging.cloud/docs/benchmarks/)

---

## 🙋 Questions?

These experiments are designed to be self-explanatory, but common questions:

**Q: Can I modify the test duration?**
A: Yes! Edit `testDurationMinutes` in each script (currently 3 min)

**Q: Can I test different message sizes?**
A: Yes! Edit `messageSize` (currently 1024 bytes = 1KB)

**Q: Why 1 minute tests (fast mode)?**
A: Quick validation for experimentation. For production, increase to 3-5 minutes for better statistical significance.

**Q: Can I test with multiple topics?**
A: Yes, but these experiments focus on single-topic scaling. Multi-topic is a different use case.

---

**Happy Experimenting! 🚀**
