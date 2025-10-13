#!/bin/bash
# Quick Consumer Scaling Test - Python OMB Version
# Tests: 1, 5, 10 consumers with 1 producer

cd /Users/lbw1125/Desktop/openmessaging-benchmark

DRIVER="examples/kafka-driver.yaml"
RESULTS_DIR="results-consumer-scaling-$(date +%Y%m%d-%H%M%S)"

mkdir -p "$RESULTS_DIR"

echo "=== Consumer Scaling Test (Python OMB) ==="
echo "Testing: 1, 5, 10 consumers"
echo "Duration: 2 minutes per test"
echo ""

# Test 3 consumer counts
CONSUMER_COUNTS=(1 5 10)

for consumer_count in "${CONSUMER_COUNTS[@]}"; do
    echo "========================================"
    echo "[$(($(date +%s)))] Testing with $consumer_count consumers"
    echo "========================================"

    # Create workload file
    WORKLOAD_FILE="$RESULTS_DIR/workload-${consumer_count}c.yaml"

    cat > "$WORKLOAD_FILE" <<EOF
name: 1p-${consumer_count}c-1kb

topics: 1
partitionsPerTopic: 10
messageSize: 1024

subscriptionsPerTopic: 1
producersPerTopic: 1
consumerPerSubscription: $consumer_count

producerRate: 1000

consumerBacklogSizeGB: 0
testDurationMinutes: 3
warmupDurationMinutes: 1

keyDistributor: NO_KEY

useRandomizedPayloads: false
randomBytesRatio: 0.0
randomizedPayloadPoolSize: 0
EOF

    # Run benchmark
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

echo "========================================"
echo "✓ All tests completed!"
echo "Results directory: $RESULTS_DIR"
echo "========================================"
echo ""
echo "Generating comparison report..."

# Generate comparison report
python3 - "$RESULTS_DIR" <<'EOF'
import json
import sys
import os
import glob

results_dir = sys.argv[1]
results = {}

print("\n=== Results Analysis ===\n")

for file_path in sorted(glob.glob(f"{results_dir}/result-*.json")):
    try:
        with open(file_path) as f:
            data = json.load(f)

        # Extract consumer count from filename
        basename = os.path.basename(file_path)
        consumers = int(basename.split('-')[1].replace('c.json', ''))

        # Calculate metrics
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
print("📊 Performance Comparison Table:")
print("")
print("| Consumers | Pub Rate | Cons Rate | Pub Latency | E2E P50 | E2E Avg | E2E P99 |")
print("|-----------|----------|-----------|-------------|---------|---------|---------|")

for consumers in sorted(results.keys()):
    r = results[consumers]
    print(f"| {consumers:9d} | {r['pub_rate']:7.1f}  | {r['cons_rate']:8.1f}  | "
          f"{r['pub_avg']:10.2f}  | {r['e2e_p50']:6.2f}  | {r['e2e_avg']:6.2f}  | {r['e2e_p99']:6.2f}  |")

# Calculate insights
print("\n📈 Key Insights:")
print("")

if 1 in results and 10 in results:
    r1 = results[1]
    r10 = results[10]

    latency_improvement = ((r1['e2e_avg'] - r10['e2e_avg']) / r1['e2e_avg'] * 100)
    throughput_change = r10['cons_rate'] - r1['cons_rate']

    print(f"  1. Latency Improvement (1→10 consumers):")
    print(f"     • E2E Avg: {r1['e2e_avg']:.2f} ms → {r10['e2e_avg']:.2f} ms ({latency_improvement:+.1f}%)")
    print(f"     • E2E P99: {r1['e2e_p99']:.2f} ms → {r10['e2e_p99']:.2f} ms")
    print("")
    print(f"  2. Throughput Stability:")
    print(f"     • Consume rate: {r1['cons_rate']:.1f} → {r10['cons_rate']:.1f} msg/s (Δ {throughput_change:+.1f})")
    print("")

    if latency_improvement > 20:
        print(f"  ✅ Significant latency improvement with more consumers!")
    elif latency_improvement > 0:
        print(f"  ✓ Moderate latency improvement with more consumers.")
    else:
        print(f"  ⚠️  No latency improvement - may already be optimal.")

if 5 in results and 10 in results:
    r5 = results[5]
    r10 = results[10]

    latency_diff = ((r5['e2e_avg'] - r10['e2e_avg']) / r5['e2e_avg'] * 100)

    print("")
    print(f"  3. Diminishing Returns (5→10 consumers):")
    print(f"     • Latency change: {latency_diff:+.1f}%")

    if abs(latency_diff) < 10:
        print(f"     ✓ Approaching optimal consumer count (10 consumers ≈ 10 partitions)")
    else:
        print(f"     → Still improving with more consumers")

print("")
print(f"💡 Recommendation: Optimal consumer count = partition count (10)")
print("")
EOF

echo "📁 Full results available in: $RESULTS_DIR/"
echo ""
