# Prometheus Curl Query Examples

## 基本命令格式

### Instant Query (查询当前值)
```bash
curl -s 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=YOUR_QUERY_HERE' | \
  python3 -m json.tool
```

### Range Query (查询时间范围)
```bash
curl -s 'http://localhost:9090/api/v1/query_range' \
  --data-urlencode 'query=YOUR_QUERY_HERE' \
  --data-urlencode 'start=1698019200' \
  --data-urlencode 'end=1698022800' \
  --data-urlencode 'step=15' | \
  python3 -m json.tool
```

---

## Kafka 查询示例 (port 9090)

### 1. CPU Usage
```bash
curl -s 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=avg(process_cpu_seconds_total{job="kafka-jmx"}) / 1000' | \
  python3 -m json.tool
```

**简化输出（只显示值）：**
```bash
curl -s 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=avg(process_cpu_seconds_total{job="kafka-jmx"}) / 1000' | \
  python3 -c 'import sys, json; d=json.load(sys.stdin); print(d["data"]["result"][0]["value"][1])'
```

### 2. JVM GC Collection Time
```bash
curl -s 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=avg(rate(jvm_gc_collection_seconds_sum{job="kafka-jmx"}[1m]))' | \
  python3 -m json.tool
```

**简化输出：**
```bash
curl -s 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=avg(rate(jvm_gc_collection_seconds_sum{job="kafka-jmx"}[1m]))' | \
  python3 -c 'import sys, json; d=json.load(sys.stdin); print(d["data"]["result"][0]["value"][1])'
```

### 3. JVM Memory Pool Usage
```bash
curl -s 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=avg(jvm_memory_pool_bytes_used{job="kafka-jmx"}) / 100000' | \
  python3 -m json.tool
```

**简化输出：**
```bash
curl -s 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=avg(jvm_memory_pool_bytes_used{job="kafka-jmx"}) / 100000' | \
  python3 -c 'import sys, json; d=json.load(sys.stdin); print(d["data"]["result"][0]["value"][1])'
```

---

## Pulsar 查询示例 (port 9091)

### 1. CPU Usage
```bash
curl -s 'http://localhost:9091/api/v1/query' \
  --data-urlencode 'query=avg(process_cpu_seconds_total{job="pulsar"}) / 1000' | \
  python3 -m json.tool
```

**简化输出：**
```bash
curl -s 'http://localhost:9091/api/v1/query' \
  --data-urlencode 'query=avg(process_cpu_seconds_total{job="pulsar"}) / 1000' | \
  python3 -c 'import sys, json; d=json.load(sys.stdin); print(d["data"]["result"][0]["value"][1])'
```

### 2. JVM GC Collection Time
```bash
curl -s 'http://localhost:9091/api/v1/query' \
  --data-urlencode 'query=avg(rate(jvm_gc_collection_seconds_sum{job="pulsar"}[1m]))' | \
  python3 -m json.tool
```

**简化输出：**
```bash
curl -s 'http://localhost:9091/api/v1/query' \
  --data-urlencode 'query=avg(rate(jvm_gc_collection_seconds_sum{job="pulsar"}[1m]))' | \
  python3 -c 'import sys, json; d=json.load(sys.stdin); print(d["data"]["result"][0]["value"][1])'
```

### 3. JVM Memory Pool Usage
```bash
curl -s 'http://localhost:9091/api/v1/query' \
  --data-urlencode 'query=avg(jvm_memory_pool_bytes_used{job="pulsar"}) / 100000' | \
  python3 -m json.tool
```

**简化输出：**
```bash
curl -s 'http://localhost:9091/api/v1/query' \
  --data-urlencode 'query=avg(jvm_memory_pool_bytes_used{job="pulsar"}) / 100000' | \
  python3 -c 'import sys, json; d=json.load(sys.stdin); print(d["data"]["result"][0]["value"][1])'
```

---

## 其他有用的查询

### 查看所有可用指标
```bash
# Kafka
curl -s 'http://localhost:9090/api/v1/label/__name__/values' | python3 -m json.tool

# Pulsar
curl -s 'http://localhost:9091/api/v1/label/__name__/values' | python3 -m json.tool
```

### 查看 targets 状态
```bash
# Kafka
curl -s 'http://localhost:9090/api/v1/targets' | python3 -m json.tool

# Pulsar
curl -s 'http://localhost:9091/api/v1/targets' | python3 -m json.tool
```

### 查询最近 5 分钟的数据（时间序列）
```bash
# Kafka CPU (最近5分钟，15秒间隔)
curl -s 'http://localhost:9090/api/v1/query_range' \
  --data-urlencode 'query=avg(process_cpu_seconds_total{job="kafka-jmx"}) / 1000' \
  --data-urlencode "start=$(date -u -d '5 minutes ago' +%s)" \
  --data-urlencode "end=$(date -u +%s)" \
  --data-urlencode 'step=15' | \
  python3 -m json.tool
```

**macOS 版本（date 命令语法不同）：**
```bash
# Kafka CPU (最近5分钟，15秒间隔)
curl -s 'http://localhost:9090/api/v1/query_range' \
  --data-urlencode 'query=avg(process_cpu_seconds_total{job="kafka-jmx"}) / 1000' \
  --data-urlencode "start=$(date -u -v-5M +%s)" \
  --data-urlencode "end=$(date -u +%s)" \
  --data-urlencode 'step=15' | \
  python3 -m json.tool
```

---

## 使用 jq 美化输出（可选）

如果安装了 `jq`，可以用它代替 `python3 -m json.tool`：

```bash
# 安装 jq
brew install jq  # macOS
# 或
apt-get install jq  # Ubuntu/Debian

# 使用 jq 查询
curl -s 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=avg(process_cpu_seconds_total{job="kafka-jmx"})' | \
  jq '.'

# 只显示值
curl -s 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=avg(process_cpu_seconds_total{job="kafka-jmx"})' | \
  jq -r '.data.result[0].value[1]'
```

---

## 监控脚本示例

创建一个简单的监控脚本：

```bash
#!/bin/bash
# monitor-kafka.sh

while true; do
  echo "=== $(date) ==="

  CPU=$(curl -s 'http://localhost:9090/api/v1/query' \
    --data-urlencode 'query=avg(process_cpu_seconds_total{job="kafka-jmx"}) / 1000' | \
    python3 -c 'import sys, json; d=json.load(sys.stdin); print(d["data"]["result"][0]["value"][1])')

  GC=$(curl -s 'http://localhost:9090/api/v1/query' \
    --data-urlencode 'query=avg(rate(jvm_gc_collection_seconds_sum{job="kafka-jmx"}[1m]))' | \
    python3 -c 'import sys, json; d=json.load(sys.stdin); print(d["data"]["result"][0]["value"][1])')

  MEM=$(curl -s 'http://localhost:9090/api/v1/query' \
    --data-urlencode 'query=avg(jvm_memory_pool_bytes_used{job="kafka-jmx"}) / 1000000' | \
    python3 -c 'import sys, json; d=json.load(sys.stdin); print(d["data"]["result"][0]["value"][1])')

  echo "CPU: $CPU"
  echo "GC:  $GC"
  echo "MEM: $MEM MB"
  echo ""

  sleep 5
done
```

---

## 错误处理

如果查询返回空结果：
```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": []
  }
}
```

可能的原因：
1. Prometheus 还没有收集到数据（等待 15-30 秒）
2. target 没有启动或不健康（检查 http://localhost:9090/targets）
3. 指标名称错误（检查可用指标列表）
4. job 标签不匹配（确认使用正确的 job 名称）
