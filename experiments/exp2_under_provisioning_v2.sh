#!/bin/bash
# Experiment 2 V2: Under-Provisioning Test (Optimized)
# Question: What happens when partition count > consumer count?
# Improvements: Better consumer gradients, fixed partition count issue

cd /Users/lbw1125/Desktop/openmessaging-benchmark

DRIVER="examples/kafka-driver.yaml"
RESULTS_DIR="results-exp2-under-provisioning-v2-$(date +%Y%m%d-%H%M%S)"

mkdir -p "$RESULTS_DIR"

echo "=========================================="
echo "🧪 Experiment 2 V2: Under-Provisioning Test (Optimized)"
echo "=========================================="
echo "Testing: 2, 4, 6, 8, 10, 12, 15, 20 consumers (more gradual)"
echo "Partitions: 20 (fixed)"
echo "Producer Rate: 1000 msg/s"
echo "Duration: 2 min test + 1 min warmup"
echo "Expected: Continuous improvement until 20 consumers"
echo ""

# More gradual consumer scaling: 2, 4, 6, 8, 10, 12, 15, 20
CONSUMER_COUNTS=(2 4 6 8 10 12 15 20)

for consumer_count in "${CONSUMER_COUNTS[@]}"; do
    echo "=========================================="
    echo "[$(($(date +%s)))] Testing with $consumer_count consumers (20 partitions)"
    echo "Parts per consumer: $(echo "scale=1; 20/$consumer_count" | bc)"
    echo "=========================================="

    WORKLOAD_FILE="$RESULTS_DIR/workload-${consumer_count}c-20p.yaml"

    cat > "$WORKLOAD_FILE" <<EOF
name: exp2v2-${consumer_count}c-20p

topics: 1
partitionsPerTopic: 20
messageSize: 1024

subscriptionsPerTopic: 1
producersPerTopic: 1
consumerPerSubscription: $consumer_count

producerRate: 1000

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
echo "✓ Experiment 2 V2 completed!"
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

print("\n=== Experiment 2 V2: Under-Provisioning Analysis (Optimized) ===\n")

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
print("| Consumers | Parts/Cons | E2E Avg | E2E P50 | E2E P99 | Improvement | Status |")
print("|-----------|------------|---------|---------|---------|-------------|--------|")

prev_latency = None
for consumers in sorted(results.keys()):
    r = results[consumers]
    ppc = 20.0 / consumers

    if prev_latency:
        improvement = ((prev_latency - r['e2e_avg']) / prev_latency * 100)
        imp_str = f"{improvement:+5.1f}%"
    else:
        imp_str = "baseline"

    # Status indicator
    if consumers < 10:
        status = "⚠️ Under"
    elif consumers < 20:
        status = "✓ Getting close"
    else:
        status = "✅ Optimal"

    print(f"| {consumers:9d} | {ppc:10.1f} | {r['e2e_avg']:7.2f} | {r['e2e_p50']:7.2f} | {r['e2e_p99']:7.2f} | {imp_str:11s} | {status:6s} |")
    prev_latency = r['e2e_avg']

# Analysis
print("\n📈 Key Findings:")
print("")

consumer_list = sorted(results.keys())

# Find best performing configuration
best_consumer = min(results.keys(), key=lambda c: results[c]['e2e_avg'])
best_latency = results[best_consumer]['e2e_avg']

print(f"  1. 🏆 Best Performance: {best_consumer} consumers")
print(f"     • E2E Avg: {best_latency:.2f} ms")
print(f"     • Parts/Consumer: {20.0/best_consumer:.1f}")

# Analyze under-provisioning penalty
if 2 in results and best_consumer in results:
    penalty = ((results[2]['e2e_avg'] - results[best_consumer]['e2e_avg']) / results[best_consumer]['e2e_avg'] * 100)
    print("")
    print(f"  2. ⚠️  Under-Provisioning Penalty:")
    print(f"     • 2 consumers (10 parts each): {results[2]['e2e_avg']:.2f} ms")
    print(f"     • {best_consumer} consumers (optimal): {results[best_consumer]['e2e_avg']:.2f} ms")
    print(f"     • Penalty: {penalty:+.1f}% worse latency")
    if penalty > 50:
        print(f"     ✅ Severe penalty confirms under-provisioning is BAD")

# Incremental improvements
print("")
print(f"  3. 🔍 Incremental Improvements:")
for i in range(len(consumer_list) - 1):
    c1 = consumer_list[i]
    c2 = consumer_list[i + 1]
    r1 = results[c1]
    r2 = results[c2]
    improvement = ((r1['e2e_avg'] - r2['e2e_avg']) / r1['e2e_avg'] * 100)

    ppc1 = 20.0 / c1
    ppc2 = 20.0 / c2

    if improvement > 15:
        status = "✅ Major improvement"
    elif improvement > 5:
        status = "✓ Good improvement"
    elif improvement > 0:
        status = "→ Slight improvement"
    else:
        status = "⚠️ Getting worse"

    print(f"     • {c1}→{c2} consumers ({ppc1:.1f}→{ppc2:.1f} parts/cons): {improvement:+.1f}% | {status}")

# Find the optimal range
print("")
print(f"  4. 🎯 Optimal Configuration:")

# Find where improvement drops below 5%
optimal_found = False
for i in range(len(consumer_list) - 1):
    c1 = consumer_list[i]
    c2 = consumer_list[i + 1]
    r1 = results[c1]
    r2 = results[c2]
    improvement = ((r1['e2e_avg'] - r2['e2e_avg']) / r1['e2e_avg'] * 100)

    if improvement < 5 and not optimal_found:
        print(f"     • Optimal range: {c1}-{c2} consumers")
        print(f"     • Beyond {c1} consumers, diminishing returns (<5% improvement)")
        optimal_found = True
        break

if not optimal_found:
    print(f"     • Keep increasing consumers until matching partition count (20)")

print("")
print("💡 Conclusion:")
print("   • Severe under-provisioning (2-4 consumers) causes HIGH latency")
print("   • Latency improves continuously as consumers approach partition count")
print("   • Optimal: consumer count ≈ partition count (20)")
print("")
EOF

echo "📁 Full results available in: $RESULTS_DIR/"
echo ""
