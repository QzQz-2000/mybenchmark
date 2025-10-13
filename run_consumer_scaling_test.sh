#!/bin/bash
# Test: How does consumer count affect performance with 1 producer?
# Measures: throughput, latency, CPU usage

set -e

DRIVER="examples/kafka-driver.yaml"
WORKLOAD_TEMPLATE="workloads/1producer-variable-consumers-1kb.yaml"
RESULTS_DIR="results-consumer-scaling-$(date +%Y%m%d-%H%M%S)"

mkdir -p "$RESULTS_DIR"

echo "=== Consumer Scaling Test ==="
echo "Fixed: 1 Producer, 1000 msg/s, 1KB messages, 10 partitions"
echo "Variable: Consumer count"
echo ""

# Test different consumer counts
CONSUMER_COUNTS=(1 5 10 20 50)

for consumer_count in "${CONSUMER_COUNTS[@]}"; do
    echo "========================================"
    echo "Testing with $consumer_count consumers"
    echo "========================================"

    # Create temporary workload file with specific consumer count
    TEMP_WORKLOAD=$(mktemp /tmp/workload-${consumer_count}c.XXXXXX.yaml)

    cat > "$TEMP_WORKLOAD" <<EOF
name: 1producer-${consumer_count}consumers-1kb

topics: 1
partitionsPerTopic: 10
messageSize: 1024
payloadFile: "payload/payload-1Kb.data"

subscriptionsPerTopic: 1
producersPerTopic: 1
consumerPerSubscription: $consumer_count

producerRate: 1000

consumerBacklogSizeGB: 0
testDurationMinutes: 5
warmupDurationMinutes: 1

keyDistributor: "NO_KEY"
EOF

    # Run benchmark
    OUTPUT_FILE="$RESULTS_DIR/1p-${consumer_count}c-1kb.json"

    python -m benchmark.benchmark \
        -d "$DRIVER" \
        -o "$OUTPUT_FILE" \
        "$TEMP_WORKLOAD"

    echo "Results saved to: $OUTPUT_FILE"
    echo ""

    # Clean up temp file
    rm "$TEMP_WORKLOAD"

    # Wait a bit between tests
    sleep 10
done

echo "========================================"
echo "All tests completed!"
echo "Results directory: $RESULTS_DIR"
echo "========================================"

# Generate comparison report
echo ""
echo "=== Performance Comparison ==="
python3 <<EOF
import json
import glob
import os

results = {}
for file in glob.glob("$RESULTS_DIR/*.json"):
    with open(file) as f:
        data = json.load(f)
        consumers = int(os.path.basename(file).split('-')[1].replace('c', ''))

        # Calculate averages
        avg_pub_rate = sum(data['publishRate']) / len(data['publishRate'])
        avg_cons_rate = sum(data['consumeRate']) / len(data['consumeRate'])
        avg_e2e_latency = data['aggregatedEndToEndLatencyAvg']
        p99_e2e_latency = data['aggregatedEndToEndLatency99pct']

        results[consumers] = {
            'pub_rate': avg_pub_rate,
            'cons_rate': avg_cons_rate,
            'e2e_latency_avg': avg_e2e_latency,
            'e2e_latency_p99': p99_e2e_latency
        }

# Print comparison table
print("| Consumers | Pub Rate | Cons Rate | E2E Avg Latency | E2E P99 Latency |")
print("|-----------|----------|-----------|-----------------|-----------------|")
for consumers in sorted(results.keys()):
    r = results[consumers]
    print(f"| {consumers:9d} | {r['pub_rate']:8.1f} | {r['cons_rate']:9.1f} | {r['e2e_latency_avg']:15.2f} | {r['e2e_latency_p99']:15.2f} |")
EOF

echo ""
echo "Test complete! Check $RESULTS_DIR for detailed results."
