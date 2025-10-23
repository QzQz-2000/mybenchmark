这是一些设计基准测试（如使用 OpenMessaging Benchmark）的实验场景和步骤，旨在衡量消息中间件在不同通信模型下的性能。

根据您的历史信息，您的测试环境是 **Kafka**，所以以下设计将以 Kafka 的术语和能力为基础。

### 实验设计核心目标

测试目标通常围绕以下三个核心指标设计：

1.  **最大吞吐量 (Max Throughput)：** 系统在不损失可靠性（例如，保持低丢包率）的前提下，能处理的最大消息速率（`msg/s` 或 `MB/s`）。
2.  **延迟特性 (Latency Profile)：** 在特定高负载下，消息从生产者发送到最终被消费者确认接收所需的时间（端到端延迟）。
3.  **可扩展性 (Scalability)：** 增加分区、生产者或消费者数量时，系统吞吐量的变化趋势。

---

## 实验场景一：一对一（Point-to-Point）

这是最基础、最理想的场景，用于测试系统在无竞争下的**单通道极限性能**。

### 场景定义

* $1$ 个主题 ($1$ Topic)
* $1$ 个分区 ($1$ Partition)
* $1$ 个生产者 ($1$ Producer)
* $1$ 个消费者 ($1$ Consumer)

### 实验设计（固定配置）

| 参数 | 值 | 说明 |
| :--- | :--- | :--- |
| `TOPICS` | $1$ | 只有一个主题。 |
| `PARTITIONS_PER_TOPIC` | $1$ | 确保所有流量都在单个通道上。 |
| `PRODUCERS_PER_TOPIC` | $1$ | 单个发送者。 |
| `CONSUMER_PER_SUBSCRIPTION` | $1$ | 单个接收者。 |
| `FIXED_MESSAGE_SIZE` | $1\text{ KB}$ | 保持消息大小固定。 |

### 实验步骤（控制变量）

使用您之前的**速率扩展性测试脚本**（控制 `PRODUCER_RATE`，每次翻倍），但保持上述 $1:1$ 配置。

1.  **低速测试：** $1000 \text{ msg/s}, 2000 \text{ msg/s}, \dots$
2.  **找出瓶颈：** 持续增加速率，直到发现 $50\% \text{ Latency}$ 开始急剧恶化，或者 `ProdRate` 无法跟上 `PRODUCER_RATE`。

### 衡量重点

* **单核性能上限：** 理论上，此场景主要受制于单个 Broker 核或单个分区处理的性能。
* **延迟基线：** 在接近 $100\%$ 利用率时，系统的最小端到端延迟基线是多少。

---

## 实验场景二：多对一（Fan-in）

用于测试 Broker **单个分区或主题**在面对多个生产者并发写入时的**写入竞争和分区锁效率**。

### 场景定义

* $1$ 个主题
* $1$ 个分区（或少量分区）
* $N$ 个生产者 ($N > 1$)
* $1$ 个消费者（或少量消费者）

### 实验设计（固定配置）

| 参数 | 值 | 说明 |
| :--- | :--- | :--- |
| `TOPICS` | $1$ | 只有一个主题。 |
| `PARTITIONS_PER_TOPIC` | $1$ 或 $4$ | 保持分区数很小，以确保写入竞争。 |
| `PRODUCERS_PER_TOPIC` | $4, 8, 16, \dots$ | 改变生产者的数量。 |
| `CONSUMER_PER_SUBSCRIPTION` | $1$ | 保持消费者数量不变。 |
| `PRODUCER_RATE` | 固定高速率 | 总目标吞吐量应接近系统上限，以保证写入压力。 |

### 实验步骤（控制变量）

1.  **固定总速率：** 将总的 `PRODUCER_RATE` 固定为一个已知的**高负载值**（例如，前一个实验中 $95\% \text{ Latency}$ 开始恶化的速率）。
2.  **改变生产者数量：** 运行 $4\text{ P} : 1\text{ C}, 8\text{ P} : 1\text{ C}, 16\text{ P} : 1\text{ C}$ 等配置。

### 衡量重点

* **写入瓶颈：** 增加生产者数量时，系统的总 `ProdRate` 能否保持稳定，以及 $99\% \text{ Latency}$ 的增长趋势。如果延迟显著上升但吞吐量未增加，说明 Broker 在处理高并发写入时的**锁竞争**严重。

---

## 实验场景三：一对多（Fan-out）

用于测试 Broker **单个主题**在处理大量消费者时的**读取可扩展性和网络负载**。

### 场景定义

* $1$ 个主题
* $N$ 个分区
* $1$ 个生产者
* $N$ 个消费者（在同一个订阅组内或不同的订阅组内）

### 实验设计（控制变量）

这个场景有两种常见的测试方法：

#### A. 组内扩展（横向扩展消费能力）

| 参数 | 值 | 说明 |
| :--- | :--- | :--- |
| `PARTITIONS_PER_TOPIC` | $4, 8, 16, \dots$ | 分区数必须**等于或大于**消费者的数量。 |
| `PRODUCERS_PER_TOPIC` | $1$ | 单个生产者。 |
| `SUBSCRIPTIONS_PER_TOPIC` | $1$ | 只有一个消费组。 |
| `CONSUMER_PER_SUBSCRIPTION` | $4, 8, 16, \dots$ | 逐渐增加消费者数量，每个消费者消费 $1$ 个分区。 |
| `PRODUCER_RATE` | 固定高速率 | 保持高写入压力，总速率应接近系统上限。 |

**衡量重点：** 验证 Kafka 的设计（消费者数量 $\le$ 分区数量时）是否能实现**线性的消费能力扩展**，`ConsRate` 是否能跟随分区和消费者的增加而线性增加。

#### B. 跨组扩展（广播/多订阅）

| 参数 | 值 | 说明 |
| :--- | :--- | :--- |
| `PARTITIONS_PER_TOPIC` | $1$ 或 $4$ | 保持分区数固定。 |
| `SUBSCRIPTIONS_PER_TOPIC` | $2, 4, 8, \dots$ | 增加独立的消费组数量（每个组都接收全部数据）。 |
| `CONSUMER_PER_SUBSCRIPTION` | $1$ | 每个消费组只有 $1$ 个消费者。 |
| `PRODUCER_RATE` | 固定速率 | 保持一个中等到高写入速率。 |

**衡量重点：** 测试 Broker 在向多组（多套客户端）发送**相同数据**时的**网络带宽和 CPU 压力**。总网络出带宽会是 $N \times (\text{ProdThr})$。

---

## 实验场景四：多对多（Many-to-Many）

这是最真实的生产环境场景，用于测试系统的**总体极限容量、平衡和鲁棒性**。

### 场景定义

* $N$ 个主题
* $N$ 个分区
* $N$ 个生产者
* $N$ 个消费者

### 实验设计（固定配置）

| 参数 | 值 | 说明 |
| :--- | :--- | :--- |
| `TOPICS` | $4$ | 多个主题以模拟多业务线。 |
| `PARTITIONS_PER_TOPIC` | $4$ | 每个主题有 $4$ 个分区。 |
| `PRODUCERS_PER_TOPIC` | $4$ | 每个主题有 $4$ 个生产者。 |
| `CONSUMER_PER_SUBSCRIPTION` | $4$ | 每个主题有 $4$ 个消费者。 |
| **总分区数** | $16$ | ( $4$ Topics $\times 4$ Partitions ) |
| **总生产者数** | $16$ | ( $4$ Topics $\times 4$ Producers ) |
| **总消费者数** | $16$ | ( $4$ Topics $\times 4$ Consumers ) |

### 实验步骤（控制变量）

使用**消息大小测试**或**速率扩展性测试**脚本。

1.  **改变消息大小：** 保持 $30000 \text{ msg/s}$ 的总速率（即 $1875 \text{ msg/s}$/Producer），然后运行 $8\text{ B}$ 到 $128\text{ KB}$ 的消息大小测试。这将显示系统的**总体吞吐量上限**（$MB/s$）。
2.  **改变总速率：** 固定 $1\text{ KB}$ 消息大小，然后逐渐增加每个生产者的 `PRODUCER_RATE`，直到系统达到瓶颈。

### 衡量重点

* **总体容量：** 在高并发和高分区数量下，系统的最大稳定吞吐量是多少。
* **资源利用：** 监控 Broker 的 CPU、磁盘 I/O 和网络使用率，确定整体瓶颈所在。


测试延迟、吞吐量以及系统资源（如 CPU、I/O）需要结合使用**基准测试工具（OpenMessaging Benchmark）和系统监控工具**。

以下是如何测试这些指标的详细步骤和所需工具：

## 步骤一：测试应用指标（延迟 & 吞吐量）

这是通过您正在使用的 **OpenMessaging Benchmark (OMB)** 工具来完成的。

### 1\. 吞吐量 (Throughput)

OMB 会在测试结束时自动计算和报告以下指标：

| 指标 | 含义 | 如何在结果中体现 (`.json`) |
| :--- | :--- | :--- |
| **生产速率** (Publish Rate) | 每秒成功发送的消息条数（`msg/s`）。 | `aggregatedPublishRateAvg` |
| **消费速率** (Consume Rate) | 每秒成功消费的消息条数（`msg/s`）。 | `aggregatedConsumeRateAvg` |
| **生产吞吐量** (Publish Throughput) | 每秒发送的数据量（`MB/s`）。 | `aggregatedPublishThroughputAvg` |
| **消费吞吐量** (Consume Throughput) | 每秒消费的数据量（`MB/s`）。 | `aggregatedConsumeThroughputAvg` |

### 2\. 延迟 (Latency)

OMB 通过在消息中嵌入时间戳，计算消息从生产者发送到被消费者确认接收所需的时间，并计算**端到端延迟**：

| 指标 | 含义 | 如何在结果中体现 (`.json`) |
| :--- | :--- | :--- |
| **平均延迟** (Average Latency) | 所有消息的平均端到端延迟。 | `aggregatedEndToEndLatencyAvg` |
| **百分位延迟** (Percentile Latency) | 衡量长尾效应，通常关注 $95\% / 99\% / 99.9\% \text{ Latency}$。 | `aggregatedEndToEndLatency99pct` 等 |

**操作：** 运行您的 Bash 脚本，OMB 会自动在 `result_runX.json` 中输出这些数据，您最后的 Python 分析脚本就是用来汇总这些数据的。

-----

## 步骤二：测试系统指标（CPU, I/O, Network）

这需要使用**独立的系统监控工具**来监控 Kafka Broker 运行的服务器或容器。

**目标：** 在基准测试运行期间，持续收集 Broker 进程的资源使用情况。

### 1\. CPU 使用率

#### 工具：`top` / `htop` (Linux) 或 `docker stats` (容器)

  * **如果 Kafka 运行在 Docker 容器中：**

    ```bash
    docker stats KAFKA_CONTAINER_NAME --no-stream
    # KAFKA_CONTAINER_NAME 即您脚本中的 "kafka"
    ```

    `docker stats` 会直接显示容器的 CPU 百分比、内存使用情况和网络 I/O。

  * **如果 Kafka 运行在宿主机上：**
    使用 `htop` 或 `top` 实时查看，或使用 `pidstat` 追踪特定进程的 CPU 使用率：

    ```bash
    # 每 5 秒记录一次 Broker 进程的 CPU 负载
    pidstat -u -p <Kafka Broker 进程ID> 5
    ```

#### 关注指标：

  * **CPU% User：** 用户态 CPU 使用率（Kafka 进程本身执行业务逻辑的开销）。
  * **CPU% System：** 内核态 CPU 使用率（文件 I/O、网络传输等内核操作的开销）。

### 2\. 磁盘 I/O (Disk I/O)

#### 工具：`iostat` / `iotop`

`iostat` 可以查看整体磁盘的读写性能。

```bash
iostat -x 1
# -x 显示扩展统计信息，1 代表每秒刷新一次
```

#### 关注指标：

  * **`r/s` / `w/s`：** 每秒的读/写请求次数（衡量 I/O 效率）。
  * **`rMB/s` / `wMB/s`：** 每秒的读/写数据量（吞吐量）。
  * **`util%` (或 `%util`)：** 磁盘利用率。如果接近 $100\%$，说明磁盘已成为瓶颈。
  * **`await`：** I/O 请求的平均等待时间（衡量 I/O 延迟）。

### 3\. 网络带宽 (Network Bandwidth)

#### 工具：`sar` / `ifstat` / `docker stats`

如果您使用 `docker stats`，它会直接显示容器的网络输入 (`NET I/O` 的 $\text{Rx}$) 和输出 ($\text{Tx}$) 速率。

  * **宿主机监控：**
    ```bash
    sar -n DEV 1
    # 查看网络接口统计信息，每 1 秒一次
    ```

#### 关注指标：

  * **`rxkB/s`：** 每秒接收的 $KB$ 数（生产者流入 Broker 的数据）。
  * **`txkB/s`：** 每秒发送的 $KB$ 数（Broker 流向消费者或副本的数据）。

-----

## 最佳实践：整合监控（推荐）

在专业基准测试中，推荐使用 **Prometheus + Grafana** 监控栈来自动收集和可视化所有指标，包括 Kafka 自身的 JMX 指标（如堆内存、GC 时间、Broker 请求队列长度等）。

**操作流程建议：**

1.  **启动监控：** 在运行基准测试前，启动 `docker stats` 或 `iostat` 等工具，并开始记录数据。
2.  **运行测试：** 启动您的 OMB 脚本。
3.  **对齐数据：** 在 OMB 测试结束（通常在稳定运行阶段的末尾）时，记录系统指标的平均值或峰值，并将其与 OMB 的吞吐量和延迟结果进行对比分析。

通过这种方式，您不仅知道 $2000 \text{ msg/s}$ 的延迟是 $752 \text{ ms}$，还能知道这是在 Broker CPU 利用率 $40\%$、磁盘 I/O $10 \text{ MB/s}$ 的情况下实现的，从而准确判断系统瓶颈。