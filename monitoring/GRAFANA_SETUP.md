# Grafana Dashboard 导入指南

## 前提条件

确保以下服务正在运行：
```bash
docker-compose -f docker-compose-kafka.yml up -d
```

服务访问地址：
- Grafana: http://localhost:3000
- Prometheus: http://localhost:9090
- Kafka JMX Exporter: http://localhost:5556/metrics

## 导入 Kafka Dashboard

### 步骤 1: 登录 Grafana

1. 打开浏览器访问 http://localhost:3000
2. 使用默认凭据登录：
   - 用户名: `admin`
   - 密码: `admin`
3. 首次登录时可能会提示修改密码，可以选择跳过

### 步骤 2: 添加 Prometheus 数据源

1. 点击左侧菜单的齿轮图标 (⚙️) -> **Data Sources**
2. 点击 **Add data source**
3. 选择 **Prometheus**
4. 配置如下：
   - **Name**: `prometheus` (必须使用这个名称)
   - **URL**: `http://prometheus:9090`
   - **Access**: `Server (default)`
5. 滚动到底部，点击 **Save & Test**
6. 应该看到绿色提示 "Data source is working"

### 步骤 3: 导入 Kafka Dashboard

有两种方式导入：

#### 方式 1: 通过 JSON 文件导入

1. 点击左侧菜单的 **+** 图标 -> **Import**
2. 点击 **Upload JSON file**
3. 选择文件: `monitoring/kafka-dashboard.json`
4. 在下一个页面中：
   - **Name**: Kafka Monitoring Dashboard (可自定义)
   - **Folder**: General (或选择其他文件夹)
   - **Prometheus**: 选择刚才创建的 `prometheus` 数据源
5. 点击 **Import**

#### 方式 2: 通过复制 JSON 导入

1. 点击左侧菜单的 **+** 图标 -> **Import**
2. 打开 `monitoring/kafka-dashboard.json` 文件
3. 复制全部内容
4. 粘贴到 **Import via panel json** 文本框
5. 点击 **Load**
6. 配置数据源为 `prometheus`
7. 点击 **Import**

## Dashboard 包含的监控指标

Dashboard 包含以下 10 个面板：

### 1. Messages In Per Second (消息流入速率)
- **指标**: `rate(kafka_server_brokertopicmetrics_messagesinpersec_count[1m])`
- **说明**: 每秒接收的消息数量

### 2. Throughput (吞吐量)
- **指标**:
  - Bytes In: `rate(kafka_server_brokertopicmetrics_bytesinpersec_count[1m])`
  - Bytes Out: `rate(kafka_server_brokertopicmetrics_bytesoutpersec_count[1m])`
- **说明**: 每秒流入/流出的字节数

### 3. Produce Request Latency (p99)
- **指标**: `kafka_network_requestmetrics_totaltimems{request="Produce",quantile="0.99"}`
- **说明**: 生产请求的 99 分位延迟（毫秒）

### 4. Fetch Request Latency (p99)
- **指标**: `kafka_network_requestmetrics_totaltimems{request="Fetch",quantile="0.99"}`
- **说明**: 消费请求的 99 分位延迟（毫秒）

### 5. JVM Memory (Heap)
- **指标**:
  - Heap Used: `jvm_memory_heap_used`
  - Heap Committed: `jvm_memory_heap_committed`
- **说明**: JVM 堆内存使用情况

### 6. GC Collections Per Second
- **指标**: `rate(jvm_gc_collection_count{gc=~".*"}[1m])`
- **说明**: 每秒 GC 次数，按 GC 类型分组

### 7. Request Rate (Produce/Fetch)
- **指标**:
  - Produce: `rate(kafka_network_request_totaltimems_total{request="Produce"}[1m])`
  - Fetch: `rate(kafka_network_request_totaltimems_total{request="Fetch"}[1m])`
- **说明**: 生产和消费请求的速率

### 8. Broker State
- **指标**: `kafka_server_kafkaserver_brokerstate`
- **说明**: Broker 状态 (0=Not Running, 1=Running, 2=Controller, 3=Running)

### 9. Active Brokers
- **指标**: `kafka_controller_kafkacontroller_activebrokercount`
- **说明**: 活跃的 Broker 数量

### 10. Disk I/O
- **指标**:
  - Read: `kafka_server_kafkaserver_linux_disk_read_bytes`
  - Write: `kafka_server_kafkaserver_linux_disk_write_bytes`
- **说明**: 磁盘读写字节数

## 验证指标数据

如果某些面板显示 "No data"，可以通过以下方式验证：

### 1. 检查 Prometheus 是否抓取到数据

访问 http://localhost:9090 并执行以下查询：

```promql
# 检查 Kafka 是否有数据
kafka_server_brokertopicmetrics_messagesinpersec_count

# 检查 JVM 内存
jvm_memory_heap_used

# 检查活跃 Broker
kafka_controller_kafkacontroller_activebrokercount
```

### 2. 检查 JMX Exporter 是否工作

```bash
curl http://localhost:5556/metrics | grep kafka
```

### 3. 检查 Prometheus Targets

访问 http://localhost:9090/targets 查看 `kafka-jmx` target 是否为 "UP" 状态

## 生成测试数据

如果没有实际流量，可以运行基准测试生成数据：

```bash
# 使用 Kafka 自带的性能测试工具
docker exec kafka kafka-producer-perf-test \
  --topic test-topic \
  --num-records 100000 \
  --record-size 1000 \
  --throughput 1000 \
  --producer-props bootstrap.servers=localhost:9092
```

docker exec kafka bash -c "export JMX_PORT=9102 && kafka-producer-perf-test \
  --topic test-topic \
  --num-records 100000 \
  --record-size 1000 \
  --throughput 1000 \
  --producer-props bootstrap.servers=localhost:9092"

## Dashboard 刷新设置

Dashboard 默认配置：
- **刷新间隔**: 10秒
- **时间范围**: 最近 15 分钟

可以在右上角调整这些设置。

## 故障排查

### 问题: Dashboard 导入后所有面板显示 "No data"

**解决方案**:
1. 确认 Prometheus 数据源配置正确（名称必须是 `prometheus`）
2. 检查 Kafka 和 JMX Exporter 容器是否运行：
   ```bash
   docker ps | grep -E "kafka|prometheus|jmx"
   ```
3. 检查 Prometheus targets: http://localhost:9090/targets

### 问题: 只有部分指标有数据

**解决方案**:
1. 某些指标需要有实际流量才会产生数据（如消息速率、请求延迟）
2. 运行测试生成流量（见上方"生成测试数据"部分）

### 问题: "Data source prometheus not found"

**解决方案**:
1. 确保已按步骤 2 添加 Prometheus 数据源
2. 数据源名称必须完全匹配 `prometheus`（小写）

## 自定义 Dashboard

导入后可以自由修改 Dashboard：
1. 点击右上角的齿轮图标 -> **Settings**
2. 可以修改面板、添加新面板、调整布局等
3. 修改后点击右上角的 **Save dashboard** 保存

## 导出 Dashboard

如果修改后想导出：
1. 点击右上角的分享图标
2. 选择 **Export**
3. 选择 **Save to file** 下载 JSON
