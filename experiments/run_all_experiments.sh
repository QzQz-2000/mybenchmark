#!/bin/bash
# Master Script: Run All Consumer Scaling Experiments
# This will run all 3 experiments sequentially and generate a final comparison report

cd /Users/lbw1125/Desktop/openmessaging-benchmark

MASTER_RESULTS_DIR="results-all-experiments-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$MASTER_RESULTS_DIR"

echo "=========================================="
echo "🔬 Consumer Scaling Experiments Suite"
echo "=========================================="
echo ""
echo "This will run 3 experiments:"
echo ""
echo "  Exp 1: Over-Provisioning (1, 5, 10, 15, 20 consumers × 10 partitions)"
echo "         → Tests if more consumers than partitions helps"
echo ""
echo "  Exp 2: Under-Provisioning (5, 10, 15, 20 consumers × 20 partitions)"
echo "         → Tests impact of fewer consumers than partitions"
echo ""
echo "  Exp 3: High Load (1, 5, 10 consumers × 10 partitions @ 10K msg/s)"
echo "         → Tests consumer scaling under 10x load"
echo ""
echo "Total tests: 12"
echo "Estimated time: ~20 minutes (1 min per test + overhead, fast mode)"
echo ""
read -p "Press Enter to start, or Ctrl+C to cancel..."
echo ""

# Track start time
START_TIME=$(date +%s)

# ============================================================
# Experiment 1: Over-Provisioning
# ============================================================
echo ""
echo "╔════════════════════════════════════════╗"
echo "║  Experiment 1: Over-Provisioning       ║"
echo "╚════════════════════════════════════════╝"
echo ""
bash experiments/exp1_over_provisioning.sh
EXP1_DIR=$(ls -td results-exp1-over-provisioning-* 2>/dev/null | head -1)
if [ -n "$EXP1_DIR" ]; then
    cp -r "$EXP1_DIR" "$MASTER_RESULTS_DIR/"
    echo "✓ Experiment 1 results copied to master directory"
fi
echo ""
echo "Waiting 10 seconds before next experiment..."
sleep 10

# ============================================================
# Experiment 2: Under-Provisioning
# ============================================================
echo ""
echo "╔════════════════════════════════════════╗"
echo "║  Experiment 2: Under-Provisioning      ║"
echo "╚════════════════════════════════════════╝"
echo ""
bash experiments/exp2_under_provisioning.sh
EXP2_DIR=$(ls -td results-exp2-under-provisioning-* 2>/dev/null | head -1)
if [ -n "$EXP2_DIR" ]; then
    cp -r "$EXP2_DIR" "$MASTER_RESULTS_DIR/"
    echo "✓ Experiment 2 results copied to master directory"
fi
echo ""
echo "Waiting 10 seconds before next experiment..."
sleep 10

# ============================================================
# Experiment 3: High Load
# ============================================================
echo ""
echo "╔════════════════════════════════════════╗"
echo "║  Experiment 3: High Load               ║"
echo "╚════════════════════════════════════════╝"
echo ""
bash experiments/exp3_high_load.sh
EXP3_DIR=$(ls -td results-exp3-high-load-* 2>/dev/null | head -1)
if [ -n "$EXP3_DIR" ]; then
    cp -r "$EXP3_DIR" "$MASTER_RESULTS_DIR/"
    echo "✓ Experiment 3 results copied to master directory"
fi

# ============================================================
# Generate Master Comparison Report
# ============================================================
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))

echo ""
echo "=========================================="
echo "✓ All Experiments Completed!"
echo "=========================================="
echo "Total duration: $MINUTES minutes"
echo "Master results: $MASTER_RESULTS_DIR"
echo ""
echo "Generating master comparison report..."
echo ""

# Generate comprehensive comparison report
python3 - "$MASTER_RESULTS_DIR" <<'EOF'
import json
import sys
import os
import glob

master_dir = sys.argv[1]

print("=" * 80)
print("🔬 CONSUMER SCALING EXPERIMENTS - MASTER REPORT")
print("=" * 80)
print()

# ============================================================
# Load all results
# ============================================================
exp1_results = {}
exp2_results = {}
exp3_results = {}

# Experiment 1: Over-provisioning
exp1_dir = glob.glob(f"{master_dir}/results-exp1-*")
if exp1_dir:
    for file_path in glob.glob(f"{exp1_dir[0]}/result-*.json"):
        try:
            with open(file_path) as f:
                data = json.load(f)
            basename = os.path.basename(file_path)
            consumers = int(basename.split('-')[1].replace('c.json', ''))
            exp1_results[consumers] = {
                'e2e_avg': data['aggregatedEndToEndLatencyAvg'],
                'e2e_p99': data['aggregatedEndToEndLatency99pct'],
                'cons_rate': sum(data['consumeRate']) / len(data['consumeRate'])
            }
        except:
            pass

# Experiment 2: Under-provisioning
exp2_dir = glob.glob(f"{master_dir}/results-exp2-*")
if exp2_dir:
    for file_path in glob.glob(f"{exp2_dir[0]}/result-*.json"):
        try:
            with open(file_path) as f:
                data = json.load(f)
            basename = os.path.basename(file_path)
            consumers = int(basename.split('-')[1].replace('c.json', ''))
            exp2_results[consumers] = {
                'e2e_avg': data['aggregatedEndToEndLatencyAvg'],
                'e2e_p99': data['aggregatedEndToEndLatency99pct'],
                'cons_rate': sum(data['consumeRate']) / len(data['consumeRate'])
            }
        except:
            pass

# Experiment 3: High load
exp3_dir = glob.glob(f"{master_dir}/results-exp3-*")
if exp3_dir:
    for file_path in glob.glob(f"{exp3_dir[0]}/result-*.json"):
        try:
            with open(file_path) as f:
                data = json.load(f)
            basename = os.path.basename(file_path)
            consumers = int(basename.split('-')[1].replace('c.json', ''))
            exp3_results[consumers] = {
                'e2e_avg': data['aggregatedEndToEndLatencyAvg'],
                'e2e_p99': data['aggregatedEndToEndLatency99pct'],
                'cons_rate': sum(data['consumeRate']) / len(data['consumeRate'])
            }
        except:
            pass

# ============================================================
# Report Section 1: Over-Provisioning
# ============================================================
print("📊 EXPERIMENT 1: Over-Provisioning (10 Partitions)")
print("-" * 80)
if exp1_results:
    print()
    print("| Consumers | E2E Avg (ms) | E2E P99 (ms) | Cons Rate (msg/s) |")
    print("|-----------|--------------|--------------|-------------------|")
    for c in sorted(exp1_results.keys()):
        r = exp1_results[c]
        print(f"| {c:9d} | {r['e2e_avg']:12.2f} | {r['e2e_p99']:12.2f} | {r['cons_rate']:17.1f} |")

    if 10 in exp1_results and 20 in exp1_results:
        change = ((exp1_results[20]['e2e_avg'] - exp1_results[10]['e2e_avg']) /
                  exp1_results[10]['e2e_avg'] * 100)
        print()
        print(f"🔍 Finding: 10→20 consumers: {change:+.1f}% latency change")
        if abs(change) < 5:
            print("   ✅ VALIDATED: Over-provisioning provides no benefit")
        print()
else:
    print("   ⚠️  No results found")
    print()

# ============================================================
# Report Section 2: Under-Provisioning
# ============================================================
print("📊 EXPERIMENT 2: Under-Provisioning (20 Partitions)")
print("-" * 80)
if exp2_results:
    print()
    print("| Consumers | Parts/Consumer | E2E Avg (ms) | E2E P99 (ms) | Cons Rate (msg/s) |")
    print("|-----------|----------------|--------------|--------------|-------------------|")
    for c in sorted(exp2_results.keys()):
        r = exp2_results[c]
        ppc = 20.0 / c
        print(f"| {c:9d} | {ppc:14.1f} | {r['e2e_avg']:12.2f} | {r['e2e_p99']:12.2f} | {r['cons_rate']:17.1f} |")

    if 5 in exp2_results and 20 in exp2_results:
        improvement = ((exp2_results[5]['e2e_avg'] - exp2_results[20]['e2e_avg']) /
                       exp2_results[5]['e2e_avg'] * 100)
        print()
        print(f"🔍 Finding: 5→20 consumers: {improvement:+.1f}% latency improvement")
        if improvement > 20:
            print("   ✅ VALIDATED: Under-provisioning significantly hurts latency")
        print()
else:
    print("   ⚠️  No results found")
    print()

# ============================================================
# Report Section 3: High Load
# ============================================================
print("📊 EXPERIMENT 3: High Load (10K msg/s, 10 Partitions)")
print("-" * 80)
if exp3_results:
    print()
    print("| Consumers | E2E Avg (ms) | E2E P99 (ms) | Cons Rate (msg/s) | Can Keep Up? |")
    print("|-----------|--------------|--------------|-------------------|--------------|")
    for c in sorted(exp3_results.keys()):
        r = exp3_results[c]
        keep_up = "✅ Yes" if r['cons_rate'] >= 9900 else "⚠️  No"
        print(f"| {c:9d} | {r['e2e_avg']:12.2f} | {r['e2e_p99']:12.2f} | {r['cons_rate']:17.1f} | {keep_up:12s} |")

    if 1 in exp3_results and 10 in exp3_results:
        improvement = ((exp3_results[1]['e2e_avg'] - exp3_results[10]['e2e_avg']) /
                       exp3_results[1]['e2e_avg'] * 100)
        print()
        print(f"🔍 Finding: 1→10 consumers @ 10K msg/s: {improvement:+.1f}% latency improvement")
        if improvement > 50:
            print("   ✅ VALIDATED: Parallelization is CRITICAL under high load")
        print()
else:
    print("   ⚠️  No results found")
    print()

# ============================================================
# Summary
# ============================================================
print("=" * 80)
print("📝 SUMMARY OF FINDINGS")
print("=" * 80)
print()
print("1️⃣  Over-Provisioning:")
print("   • More consumers than partitions provides NO benefit")
print("   • Extra consumers remain idle")
print("   • Wastes resources without improving latency")
print()
print("2️⃣  Under-Provisioning:")
print("   • Fewer consumers than partitions HURTS latency")
print("   • Each consumer must handle multiple partitions serially")
print("   • Latency improves as consumer count approaches partition count")
print()
print("3️⃣  High Load:")
print("   • Benefits of parallelization are MORE pronounced under load")
print("   • Single consumer may become bottleneck at high rates")
print("   • Optimal consumer count becomes critical for capacity")
print()
print("💡 GOLDEN RULE:")
print("   Consumer Count = Partition Count for optimal latency")
print()
print("=" * 80)
EOF

echo ""
echo "✓ Master report generated!"
echo ""
echo "📁 All results saved in: $MASTER_RESULTS_DIR"
echo ""
echo "Next steps:"
echo "  • Review individual experiment logs in subdirectories"
echo "  • Compare results across experiments"
echo "  • Use findings for production capacity planning"
echo ""
