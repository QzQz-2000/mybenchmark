#!/bin/bash
# Experiment 3: High Load Test
# Question: How does consumer scaling perform under high load?
# Setup: 1 producer @ 10,000 msg/s, variable consumers, 10 partitions
# Expected: Consumer parallelization benefits are MORE pronounced under load

cd /Users/lbw1125/Desktop/openmessaging-benchmark

DRIVER="examples/kafka-driver.yaml"
RESULTS_DIR="results-exp3-high-load-$(date +%Y%m%d-%H%M%S)"

mkdir -p "$RESULTS_DIR"

echo "=========================================="
echo "🧪 Experiment 3: High Load Test"
echo "=========================================="
echo "Testing: 1, 5, 10 consumers"
echo "Partitions: 10 (fixed)"
echo "Producer Rate: 10,000 msg/s (10x baseline)"
echo "Duration: 1 min per test (fast mode)"
echo "Expected: Bigger latency gap between 1 and 10 consumers"
echo ""

# Test consumer counts: 1, 5, 10
CONSUMER_COUNTS=(1 5 10)

for consumer_count in "${CONSUMER_COUNTS[@]}"; do
    echo "=========================================="
    echo "[$(($(date +%s)))] Testing with $consumer_count consumers @ 10K msg/s"
    echo "=========================================="

    WORKLOAD_FILE="$RESULTS_DIR/workload-${consumer_count}c-10k.yaml"

    cat > "$WORKLOAD_FILE" <<EOF
name: exp3-${consumer_count}c-10k

topics: 1
partitionsPerTopic: 10
messageSize: 1024

subscriptionsPerTopic: 1
producersPerTopic: 1
consumerPerSubscription: $consumer_count

producerRate: 10000

consumerBacklogSizeGB: 0
testDurationMinutes: 1
warmupDurationMinutes: 0

keyDistributor: NO_KEY

useRandomizedPayloads: false
randomBytesRatio: 0.0
randomizedPayloadPoolSize: 0
EOF

    OUTPUT_FILE="$RESULTS_DIR/result-${consumer_count}c.json"

    echo "Starting benchmark..."
    python -m benchmark.benchmark \
        -d "$DRIVER" \
        -o "$OUTPUT_FILE" \
        "$WORKLOAD_FILE" 2>&1 | tee "$RESULTS_DIR/log-${consumer_count}c.txt"

    if [ -f "$OUTPUT_FILE" ]; then
        echo "✓ Test completed! Results saved to: $OUTPUT_FILE"
    else
        echo "✗ Test failed! Check log: $RESULTS_DIR/log-${consumer_count}c.txt"
    fi
    echo ""

    # Wait between tests
    if [ $consumer_count -lt 10 ]; then
        echo "Waiting 10 seconds..."
        sleep 10
    fi
done

echo "=========================================="
echo "✓ Experiment 3 completed!"
echo "Results directory: $RESULTS_DIR"
echo "=========================================="
echo ""
echo "Generating analysis report..."

# Generate comparison report
python3 - "$RESULTS_DIR" <<'EOF'
import json
import sys
import os
import glob

results_dir = sys.argv[1]
results = {}

print("\n=== Experiment 3: High Load Analysis (10,000 msg/s) ===\n")

for file_path in sorted(glob.glob(f"{results_dir}/result-*.json")):
    try:
        with open(file_path) as f:
            data = json.load(f)

        basename = os.path.basename(file_path)
        consumers = int(basename.split('-')[1].replace('c.json', ''))

        avg_pub_rate = sum(data['publishRate']) / len(data['publishRate'])
        avg_cons_rate = sum(data['consumeRate']) / len(data['consumeRate'])

        results[consumers] = {
            'pub_rate': avg_pub_rate,
            'cons_rate': avg_cons_rate,
            'e2e_avg': data['aggregatedEndToEndLatencyAvg'],
            'e2e_p50': data['aggregatedEndToEndLatency50pct'],
            'e2e_p99': data['aggregatedEndToEndLatency99pct'],
            'pub_avg': data['aggregatedPublishLatencyAvg'],
            'pub_p99': data['aggregatedPublishLatency99pct']
        }

    except Exception as e:
        print(f"⚠️  Error processing {file_path}: {e}")
        continue

if not results:
    print("❌ No valid results found!")
    sys.exit(1)

# Print comparison table
print("📊 High Load Performance (10,000 msg/s):")
print("")
print("| Consumers | Pub Rate | Cons Rate | Pub Latency | E2E Avg | E2E P50 | E2E P99 |")
print("|-----------|----------|-----------|-------------|---------|---------|---------|")

for consumers in sorted(results.keys()):
    r = results[consumers]
    print(f"| {consumers:9d} | {r['pub_rate']:7.1f}  | {r['cons_rate']:8.1f}  | "
          f"{r['pub_avg']:10.2f}  | {r['e2e_avg']:6.2f}  | {r['e2e_p50']:6.2f}  | {r['e2e_p99']:6.2f}  |")

# Analysis
print("\n📈 High Load Analysis:")
print("")

if 1 in results and 10 in results:
    r1 = results[1]
    r10 = results[10]

    latency_improvement = ((r1['e2e_avg'] - r10['e2e_avg']) / r1['e2e_avg'] * 100)
    throughput_change = r10['cons_rate'] - r1['cons_rate']

    print(f"  1. Latency Under Load (1→10 consumers):")
    print(f"     • E2E Avg: {r1['e2e_avg']:.2f} ms → {r10['e2e_avg']:.2f} ms ({latency_improvement:+.1f}%)")
    print(f"     • E2E P99: {r1['e2e_p99']:.2f} ms → {r10['e2e_p99']:.2f} ms")
    print("")

    if latency_improvement > 50:
        print(f"  ✅ HUGE improvement under load! Parallelization is critical at 10K msg/s")
    elif latency_improvement > 30:
        print(f"  ✅ Significant improvement under load")
    else:
        print(f"  ⚠️  Less improvement than expected at high load")

    print("")
    print(f"  2. Throughput Capacity:")
    print(f"     • Target rate: 10,000 msg/s")
    print(f"     • Actual consume rate: {r10['cons_rate']:.1f} msg/s")

    if r10['cons_rate'] >= 9900:
        print(f"     ✅ System can handle 10K msg/s with 10 consumers")
    elif r10['cons_rate'] >= 9000:
        print(f"     ⚠️  Slightly under target (may need tuning)")
    else:
        print(f"     ❌ System bottlenecked (consume rate < 90% of target)")

    print("")
    print(f"  3. Single Consumer Bottleneck:")
    if r1['cons_rate'] < 9900:
        backlog_rate = r1['pub_rate'] - r1['cons_rate']
        print(f"     ⚠️  1 consumer cannot keep up!")
        print(f"     • Backlog accumulation: {backlog_rate:.1f} msg/s")
        print(f"     • This proves parallelization is NECESSARY at high load")
    else:
        print(f"     ✓ Single consumer can keep up (but latency is high)")

# Load baseline comparison (if user ran baseline test earlier)
print("\n📊 Comparison with Baseline (1,000 msg/s):")
print("")
print("   To compare with baseline, run the original test_consumer_scaling.sh")
print("   Expected differences:")
print("   • Latency should be 2-5x higher under 10x load")
print("   • Consumer parallelization benefits should be MORE pronounced")
print("   • Single consumer may become a bottleneck")
print("")

print("💡 Conclusion:")
print("   • Higher load amplifies the importance of consumer parallelization")
print("   • At 10K msg/s, 1 consumer may not keep up (latency spikes, backlog grows)")
print("   • Optimal consumer count (= partition count) becomes critical under load")
print("")
EOF

echo "📁 Full results available in: $RESULTS_DIR/"
echo ""
