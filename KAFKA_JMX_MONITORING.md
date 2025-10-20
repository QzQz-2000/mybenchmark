# Kafka JMX 监控指南

本指南介绍如何使用 Prometheus + JMX Exporter + Grafana 来监控 Kafka 的 JMX 指标。

## 架构概览

```
┌─────────────┐      JMX:9101       ┌──────────────────┐
│   Kafka     │◄────────────────────│  JMX Exporter    │
│   Broker    │                     │  (Port 8080)     │
└─────────────┘                     └──────────────────┘
                                             │
                                             │ HTTP Scrape
                                             ▼
                                    ┌──────────────────┐
                                    │   Prometheus     │
                                    │   (Port 9090)    │
                                    └──────────────────┘
                                             │
                                             │ Query
                                             ▼
                                    ┌──────────────────┐
                                    │    Grafana       │
                                    │   (Port 3000)    │
                                    └──────────────────┘
```

## 组件说明

### 1. Kafka Broker
- **JMX 端口**: 9101
- **配置**: 已启用 JMX 远程访问，无需认证（仅用于本地测试）

### 2. JMX Exporter
- **镜像**: `bitnami/jmx-exporter:0.20.0`
- **端口**: 8080
- **功能**: 连接到 Kafka 的 JMX 端口，将 JMX 指标转换为 Prometheus 格式
- **配置文件**: `config.yml`（已存在于项目根目录）

### 3. Prometheus
- **镜像**: `prom/prometheus:v2.48.0`
- **端口**: 9090
- **功能**: 定期抓取 JMX Exporter 暴露的指标并存储
- **配置文件**: `prometheus.yml`
- **抓取间隔**: 30秒

### 4. Grafana
- **镜像**: `grafana/grafana:10.2.0`
- **端口**: 3000
- **默认账号**: admin / admin
- **功能**: 可视化 Prometheus 中的 Kafka 指标

## 快速启动

### 1. 启动所有服务

```bash
cd /Users/lbw1125/Desktop/openmessaging-benchmark
docker-compose up -d
```

### 2. 验证服务状态

```bash
# 查看所有容器状态
docker-compose ps

# 应该看到以下容器都在运行：
# - zookeeper
# - kafka
# - kafka-jmx-exporter
# - prometheus
# - grafana
```

### 3. 访问各个服务

| 服务 | URL | 说明 |
|------|-----|------|
| **JMX Exporter** | http://localhost:8080/metrics | 查看 Kafka JMX 指标的 Prometheus 格式 |
| **Prometheus** | http://localhost:9090 | Prometheus UI，可以执行 PromQL 查询 |
| **Grafana** | http://localhost:3000 | Grafana 可视化界面（admin/admin） |
| **Kafka** | localhost:9092 | Kafka Broker 连接地址 |

## 监控指标说明

### 核心指标

已通过 `config.yml` 配置导出以下 Kafka 关键指标：

#### 1. 吞吐量指标
- `kafka_server_brokertopicmetrics_bytesin_total` - 每秒流入字节数
- `kafka_server_brokertopicmetrics_bytesout_total` - 每秒流出字节数
- `kafka_server_brokertopicmetrics_totalproducerequests_total` - 生产请求总数
- `kafka_server_brokertopicmetrics_totalfetchrequests_total` - 消费请求总数

#### 2. 请求延迟指标
- `kafka_network_requestmetrics_totaltimems` - 请求总耗时（包含多个百分位数）
  - `request="Produce"` - 生产请求延迟
  - `request="FetchConsumer"` - 消费请求延迟
  - `request="FetchFollower"` - Follower 同步延迟

#### 3. 队列指标
- `kafka_server_kafkarequesthandlerpool_requestqueuesize` - 请求队列大小

#### 4. 副本管理指标
- `kafka_server_replicamanager_partitioncount` - 分区总数
- `kafka_server_replicamanager_leadercount` - Leader 分区数
- `kafka_server_replicamanager_isrexpands_total` - ISR 扩展次数
- `kafka_server_replicamanager_isrshrinks_total` - ISR 收缩次数

#### 5. 日志指标（按 Topic/Partition）
- `kafka_log_log_logendoffset` - 日志结束偏移量
- `kafka_log_log_logstartoffset` - 日志起始偏移量
- `kafka_log_log_size` - 日志大小（字节）

#### 6. JVM 指标
- `kafka_java_lang_memory_heapmemoryusage_used` - 堆内存使用量
- `kafka_java_lang_memory_heapmemoryusage_max` - 堆内存最大值
- `kafka_java_lang_operatingsystem_processcpuload` - 进程 CPU 使用率
- `kafka_java_lang_threading_threadcount` - 线程数
- `kafka_java_lang_garbagecollector_collectioncount` - GC 次数

## 使用 Grafana Dashboard

### 访问预配置的 Dashboard

1. 打开浏览器访问 http://localhost:3000
2. 使用默认账号登录：
   - 用户名: `admin`
   - 密码: `admin`
3. 首次登录会要求修改密码，可以跳过
4. 在左侧菜单点击 "Dashboards"
5. 找到并打开 "Kafka JMX Overview" Dashboard

### Dashboard 包含的面板

预配置的 Dashboard 包含以下监控面板：

1. **Broker Throughput (Bytes/sec)** - Kafka 吞吐量（字节/秒）
2. **Request Rate (Requests/sec)** - 请求速率
3. **Total Partitions** - 总分区数
4. **Leader Partitions** - Leader 分区数
5. **Request Queue Size** - 请求队列大小
6. **Request Latency P99 (ms)** - 请求延迟 P99
7. **JVM Heap Memory Usage (%)** - JVM 堆内存使用率
8. **CPU Usage** - CPU 使用率
9. **ISR Expansion/Shrink Rate** - ISR 扩展/收缩速率
10. **GC Collection Rate** - GC 频率

## Prometheus 查询示例

在 Prometheus UI (http://localhost:9090) 中可以执行以下查询：

### 吞吐量查询

```promql
# 每秒流入速率（字节/秒）
rate(kafka_server_brokertopicmetrics_bytesin_total[1m])

# 每秒流出速率（字节/秒）
rate(kafka_server_brokertopicmetrics_bytesout_total[1m])

# 每秒生产请求数
rate(kafka_server_brokertopicmetrics_totalproducerequests_total[1m])
```

### 延迟查询

```promql
# Produce 请求 P99 延迟
kafka_network_requestmetrics_totaltimems{request="Produce",quantile="0.99"}

# Fetch 请求平均延迟
kafka_network_requestmetrics_totaltimems{request="FetchConsumer",quantile="0.5"}
```

### JVM 查询

```promql
# 堆内存使用率
100 * kafka_java_lang_memory_heapmemoryusage_used / kafka_java_lang_memory_heapmemoryusage_max

# GC 频率
rate(kafka_java_lang_garbagecollector_collectioncount[1m])
```

### 分区和副本查询

```promql
# 查看所有分区数
kafka_server_replicamanager_partitioncount

# 查看 Leader 分区数
kafka_server_replicamanager_leadercount

# ISR 扩展速率
rate(kafka_server_replicamanager_isrexpands_total[1m])
```

## 运行基准测试并查看指标

### 1. 运行基准测试

```bash
# 使用单个 topic 测试
python -m benchmark \
  --driver-config examples/kafka-driver.yaml \
  --workload workloads/1-topic-16-partitions-1kb.yaml

# 或使用分布式测试
python -m benchmark \
  --driver-config examples/kafka-driver.yaml \
  --workload examples/distributed-workload.yaml \
  --workers examples/distributed-workers.yaml
```

### 2. 在 Grafana 中观察指标

运行测试时，可以在 Grafana Dashboard 中实时观察：
- 吞吐量的变化
- 请求延迟的分布
- JVM 内存和 CPU 的使用情况
- 队列深度的变化

### 3. 在 Prometheus 中查询历史数据

测试完成后，可以在 Prometheus 中查询历史数据，分析性能特征。

## 故障排查

### 检查 JMX Exporter 是否正常工作

```bash
# 查看 JMX Exporter 日志
docker logs kafka-jmx-exporter

# 测试 JMX Exporter 端点
curl http://localhost:8080/metrics | grep kafka_
```

如果看到大量 `kafka_` 开头的指标，说明 JMX Exporter 工作正常。

### 检查 Prometheus 是否正常抓取

1. 访问 http://localhost:9090/targets
2. 查看 `kafka-jmx-exporter` target 的状态
3. 状态应该是 "UP"，如果是 "DOWN" 则检查网络连接

### 检查 Kafka JMX 端口

```bash
# 进入 Kafka 容器
docker exec -it kafka bash

# 检查 JMX 端口是否监听
netstat -ln | grep 9101
```

### 常见问题

**Q: Grafana 无法连接到 Prometheus**
A: 检查 Prometheus 容器是否正常运行：`docker-compose ps prometheus`

**Q: JMX Exporter 返回 404**
A: 访问 http://localhost:8080/metrics（注意不是根路径）

**Q: 指标为空或没有数据**
A: 确保 Kafka 有流量（运行基准测试），JMX 指标只有在有活动时才会产生数据

**Q: Grafana Dashboard 显示 "No Data"**
A:
1. 检查 Prometheus 数据源配置是否正确
2. 运行基准测试以产生数据
3. 调整时间范围（右上角）到最近 15 分钟

## 停止和清理

### 停止所有服务

```bash
docker-compose down
```

### 清理所有数据（包括 Prometheus 和 Grafana 数据）

```bash
docker-compose down -v
```

## 自定义配置

### 添加新的 JMX 指标

编辑 `config.yml`，参考现有规则添加新的 JMX bean 映射。格式如下：

```yaml
- pattern: 'kafka.server<type=(.+), name=(.+)><>Value'
  name: kafka_server_$1_$2
  type: GAUGE
```

### 修改 Prometheus 抓取频率

编辑 `prometheus.yml`，修改 `scrape_interval` 参数：

```yaml
scrape_configs:
  - job_name: 'kafka-jmx-exporter'
    scrape_interval: 15s  # 从 30s 改为 15s
```

### 创建自定义 Grafana Dashboard

1. 在 Grafana 中手动创建 Dashboard
2. 导出 JSON 定义
3. 保存到 `grafana/provisioning/dashboards/` 目录
4. 重启 Grafana 容器：`docker-compose restart grafana`

## 生产环境注意事项

**⚠️ 当前配置仅用于开发和测试环境！**

在生产环境中使用时，需要注意：

1. **启用 JMX 认证**: 当前 JMX 无需认证，生产环境必须启用
2. **使用 SSL/TLS**: 加密 JMX 和 Prometheus 通信
3. **限制网络访问**: 使用防火墙规则限制对监控端口的访问
4. **数据持久化**: 为 Prometheus 配置外部存储或长期保留策略
5. **告警配置**: 在 Prometheus 中配置 AlertManager 进行告警
6. **Grafana 安全**: 修改默认密码，配置 LDAP/OAuth 认证

## 参考资料

- [Kafka JMX Metrics Documentation](https://kafka.apache.org/documentation/#monitoring)
- [Prometheus JMX Exporter](https://github.com/prometheus/jmx_exporter)
- [Grafana Documentation](https://grafana.com/docs/)
- [OpenMessaging Benchmark Documentation](./README.md)
