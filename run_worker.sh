#!/bin/bash
# Start Kafka Worker with correct PYTHONPATH

cd /Users/lbw1125/Desktop/py-openmessaging-benchmark
export PYTHONPATH=/Users/lbw1125/Desktop/py-openmessaging-benchmark

python3 workers/kafka_worker.py \
  --worker-id worker-1 \
  --driver-config configs/kafka-latency.yaml \
  --port 8080
