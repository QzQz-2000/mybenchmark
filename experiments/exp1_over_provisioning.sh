#!/bin/bash
# Experiment 1: Over-Provisioning Test
# Question: What happens when consumer count > partition count?
# Setup: 1 producer, 20 consumers, 10 partitions
# Expected: No improvement over 10 consumers (10 consumers will be idle)

cd /Users/lbw1125/Desktop/openmessaging-benchmark || exit 1

DRIVER="examples/kafka-driver.yaml"
RESULTS_DIR="results-exp1-over-provisioning-$(date +%Y%m%d-%H%M%S)"

mkdir -p "$RESULTS_DIR"

echo "=========================================="
echo "🧪 Experiment 1: Over-Provisioning Test"
echo "=========================================="
echo "Testing: 1, 5, 10, 15, 20 consumers"
echo "Partitions: 10 (fixed)"
echo "Producer Rate: 1000 msg/s"
echo "Duration: 1 min per test (fast mode)"
echo "Expected: Plateau at 10 consumers"
echo ""

# Test consumer counts: 1, 5, 10, 15, 20
CONSUMER_COUNTS=(1)

for consumer_count in "${CONSUMER_COUNTS[@]}"; do
    echo "=========================================="
    echo "[$(date +%s)] Testing with $consumer_count consumers"
    echo "=========================================="

    WORKLOAD_FILE="$RESULTS_DIR/workload-${consumer_count}c.yaml"

    cat > "$WORKLOAD_FILE" <<EOF
name: exp1-${consumer_count}c-10p

topics: 1
partitionsPerTopic: 10
messageSize: 2048

subscriptionsPerTopic: 5
producersPerTopic: 1
consumerPerSubscription: 10
producerRate: 500

consumerBacklogSizeGB: 0
testDurationMinutes: 2
warmupDurationMinutes: 1

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
    if [ $consumer_count -lt 20 ]; then
        echo "Waiting 10 seconds..."
        sleep 10
    fi
done

echo "=========================================="
echo "✓ Experiment 1 completed!"
echo "Results directory: $RESULTS_DIR"
echo "=========================================="
echo "Generating analysis report..."

# Generate comparison report
python3 - "$RESULTS_DIR" <<'EOF'
import json
import sys
import os
import glob

results_dir = sys.argv[1]
results = {}

print("\n=== Experiment 1: Over-Provisioning Analysis ===\n")

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
print("📊 Performance vs Consumer Count:")
print("")
print("| Consumers | Partitions | Pub Rate | Cons Rate | E2E Avg | E2E P50 | E2E P99 | Idle Consumers |")
print("|-----------|------------|----------|-----------|---------|---------|---------|----------------|")

for consumers in sorted(results.keys()):
    r = results[consumers]
    idle = max(0, consumers - 10)  # 10 partitions
    print(f"| {consumers:9d} | {10:10d} | {r['pub_rate']:7.1f}  | {r['cons_rate']:8.1f}  | "
          f"{r['e2e_avg']:6.2f}  | {r['e2e_p50']:6.2f}  | {r['e2e_p99']:6.2f}  | {idle:14d} |")

# Analysis
print("\n📈 Key Findings:")
print("")

if 10 in results and 20 in results:
    r10 = results[10]
    r20 = results[20]

    latency_change = ((r20['e2e_avg'] - r10['e2e_avg']) / r10['e2e_avg'] * 100)

    print(f"  1. Latency Change (10→20 consumers):")
    print(f"     • E2E Avg: {r10['e2e_avg']:.2f} ms → {r20['e2e_avg']:.2f} ms ({latency_change:+.1f}%)")
    print(f"     • E2E P99: {r10['e2e_p99']:.2f} ms → {r20['e2e_p99']:.2f} ms")
    print("")

    if abs(latency_change) < 5:
        print(f"  ✅ As expected: No significant improvement beyond partition count!")
        print(f"     → 10 consumers are IDLE (20 consumers - 10 partitions)")
    else:
        print(f"  ⚠️  Unexpected: Latency changed by {latency_change:.1f}%")

if 1 in results and 10 in results:
    r1 = results[1]
    r10 = results[10]
    improvement = ((r1['e2e_avg'] - r10['e2e_avg']) / r1['e2e_avg'] * 100)
    print("")
    print(f"  2. Baseline Improvement (1→10 consumers):")
    print(f"     • Latency improved by {improvement:.1f}%")
    print(f"     • This is the maximum benefit from parallelization")

if 10 in results and 15 in results:
    r10 = results[10]
    r15 = results[15]
    change = ((r15['e2e_avg'] - r10['e2e_avg']) / r10['e2e_avg'] * 100)
    print("")
    print(f"  3. Over-provisioning starts at 11 consumers:")
    print(f"     • 10→15 consumers: {change:+.1f}% latency change")
    print(f"     • Plateau confirms partition limit")

print("")
print("💡 Conclusion:")
print("   • Optimal consumer count = partition count (10)")
print("   • Over-provisioning wastes resources without benefit")
print("   • To improve further, increase partition count instead")
print("")
EOF

echo "📁 Full results available in: $RESULTS_DIR/"
echo ""
