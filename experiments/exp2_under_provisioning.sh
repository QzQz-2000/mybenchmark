#!/bin/bash
# Experiment 2: Under-Provisioning Test
# Question: What happens when partition count > consumer count?
# Setup: 1 producer, variable consumers (5, 10, 15, 20), 20 partitions
# Expected: Latency improves as consumers approach partition count

cd /Users/lbw1125/Desktop/openmessaging-benchmark

DRIVER="examples/kafka-driver.yaml"
RESULTS_DIR="results-exp2-under-provisioning-$(date +%Y%m%d-%H%M%S)"

mkdir -p "$RESULTS_DIR"

echo "=========================================="
echo "🧪 Experiment 2: Under-Provisioning Test"
echo "=========================================="
echo "Testing: 5, 10, 15, 20 consumers"
echo "Partitions: 20 (fixed)"
echo "Producer Rate: 1000 msg/s"
echo "Duration: 1 min per test (fast mode)"
echo "Expected: Latency improves until 20 consumers"
echo ""

# Test consumer counts: 5, 10, 15, 20
CONSUMER_COUNTS=(5 10 15 20)

for consumer_count in "${CONSUMER_COUNTS[@]}"; do
    echo "=========================================="
    echo "[$(($(date +%s)))] Testing with $consumer_count consumers (20 partitions)"
    echo "=========================================="

    WORKLOAD_FILE="$RESULTS_DIR/workload-${consumer_count}c-20p.yaml"

    cat > "$WORKLOAD_FILE" <<EOF
name: exp2-${consumer_count}c-20p

topics: 1
partitionsPerTopic: 20
messageSize: 1024

subscriptionsPerTopic: 1
producersPerTopic: 1
consumerPerSubscription: $consumer_count

producerRate: 1000

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
    if [ $consumer_count -lt 20 ]; then
        echo "Waiting 10 seconds..."
        sleep 10
    fi
done

echo "=========================================="
echo "✓ Experiment 2 completed!"
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

print("\n=== Experiment 2: Under-Provisioning Analysis ===\n")

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
print("📊 Performance vs Consumer Count (20 Partitions):")
print("")
print("| Consumers | Partitions/Consumer | Pub Rate | Cons Rate | E2E Avg | E2E P50 | E2E P99 |")
print("|-----------|---------------------|----------|-----------|---------|---------|---------|")

for consumers in sorted(results.keys()):
    r = results[consumers]
    parts_per_consumer = 20.0 / consumers
    print(f"| {consumers:9d} | {parts_per_consumer:19.1f} | {r['pub_rate']:7.1f}  | {r['cons_rate']:8.1f}  | "
          f"{r['e2e_avg']:6.2f}  | {r['e2e_p50']:6.2f}  | {r['e2e_p99']:6.2f}  |")

# Analysis
print("\n📈 Key Findings:")
print("")

consumer_list = sorted(results.keys())

for i in range(len(consumer_list) - 1):
    c1 = consumer_list[i]
    c2 = consumer_list[i + 1]
    r1 = results[c1]
    r2 = results[c2]

    improvement = ((r1['e2e_avg'] - r2['e2e_avg']) / r1['e2e_avg'] * 100)

    print(f"  {i+1}. {c1}→{c2} consumers:")
    print(f"     • E2E Avg: {r1['e2e_avg']:.2f} ms → {r2['e2e_avg']:.2f} ms ({improvement:+.1f}%)")
    print(f"     • Partitions/consumer: {20/c1:.1f} → {20/c2:.1f}")

    if improvement > 15:
        print(f"     ✅ Significant improvement (still under-provisioned)")
    elif improvement > 5:
        print(f"     ✓ Moderate improvement (approaching optimal)")
    else:
        print(f"     → Minimal improvement (near optimal)")
    print("")

if 5 in results and 20 in results:
    r5 = results[5]
    r20 = results[20]
    total_improvement = ((r5['e2e_avg'] - r20['e2e_avg']) / r5['e2e_avg'] * 100)

    print(f"📊 Overall Results:")
    print(f"   • Total latency improvement (5→20 consumers): {total_improvement:.1f}%")
    print(f"   • E2E Avg: {r5['e2e_avg']:.2f} ms → {r20['e2e_avg']:.2f} ms")
    print(f"   • E2E P99: {r5['e2e_p99']:.2f} ms → {r20['e2e_p99']:.2f} ms")

print("")
print("💡 Conclusion:")
print("   • Under-provisioning hurts latency (each consumer handles multiple partitions)")
print("   • Latency should improve continuously until consumer count = partition count")
print("   • Rule of thumb: 1 consumer per partition for best latency")
print("")
EOF

echo "📁 Full results available in: $RESULTS_DIR/"
echo ""
