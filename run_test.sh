#!/bin/bash
# Run benchmark test with correct PYTHONPATH

cd /Users/lbw1125/Desktop/py-openmessaging-benchmark
export PYTHONPATH=/Users/lbw1125/Desktop/py-openmessaging-benchmark

python3 benchmark/cli.py run \
  --workload workloads/quick-test.yaml \
  --driver configs/kafka-latency.yaml \
  --workers http://localhost:8080 \
  --output results/
