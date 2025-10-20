#!/bin/bash

# =====================================================
# Kafka 容器资源监控脚本 (瞬时值版本)
# 功能：使用 docker stats 获取瞬时 CPU 和内存使用率
# =====================================================

# 定义配置参数
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="./kafka_monitor_instant_${TIMESTAMP}.log"
CSV_FILE="./kafka_monitor_instant_${TIMESTAMP}.csv"
STATS_FILE="./kafka_monitor_instant_${TIMESTAMP}_stats.txt"
CONTAINER_NAME="kafka101"
SLEEP_INTERVAL=5

# 统计变量
SAMPLE_COUNT=0
CPU_SUM=0
MEM_SUM=0
CPU_MAX=0
MEM_MAX=0
CPU_MIN=999999
MEM_MIN=999999

# 信号处理函数
cleanup() {
    echo ""
    echo "=== 收到停止信号，正在生成统计报告... ===" | tee -a "$LOG_FILE"
    generate_stats
    exit 0
}

trap cleanup SIGINT SIGTERM

# 生成统计报告
generate_stats() {
    if [ $SAMPLE_COUNT -eq 0 ]; then
        echo "没有收集到数据，无法生成统计报告。" | tee -a "$LOG_FILE"
        return
    fi

    CPU_AVG=$(echo "scale=2; $CPU_SUM / $SAMPLE_COUNT" | bc)
    MEM_AVG=$(echo "scale=2; $MEM_SUM / $SAMPLE_COUNT" | bc)

    cat > "$STATS_FILE" <<EOF
========================================
Kafka 容器资源使用统计报告 (瞬时值)
========================================
容器名称: ${CONTAINER_NAME}
监控时间: ${TIMESTAMP}
采样间隔: ${SLEEP_INTERVAL} 秒
采样次数: ${SAMPLE_COUNT}

CPU 使用率统计 (瞬时值):
  平均值: ${CPU_AVG}%
  最大值: ${CPU_MAX}%
  最小值: ${CPU_MIN}%

内存使用率统计 (瞬时值):
  平均值: ${MEM_AVG}%
  最大值: ${MEM_MAX}%
  最小值: ${MEM_MIN}%

数据文件:
  详细日志: ${LOG_FILE}
  CSV 数据: ${CSV_FILE}
  统计报告: ${STATS_FILE}
========================================
EOF

    cat "$STATS_FILE" | tee -a "$LOG_FILE"
    echo ""
    echo "统计报告已保存到: ${STATS_FILE}"
}

# 比较函数
max() {
    echo "$1 $2" | awk '{if ($1 > $2) print $1; else print $2}'
}

min() {
    echo "$1 $2" | awk '{if ($1 < $2) print $1; else print $2}'
}

# =====================================================
# 主程序开始
# =====================================================

echo "=========================================="
echo "  Kafka 容器资源监控脚本 (瞬时值版本)"
echo "=========================================="
echo "容器名称: ${CONTAINER_NAME}"
echo "采样间隔: ${SLEEP_INTERVAL} 秒"
echo "日志文件: ${LOG_FILE}"
echo "CSV 文件: ${CSV_FILE}"
echo "按 Ctrl+C 停止监控并生成统计报告"
echo "=========================================="
echo ""

# 检查容器是否存在
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "错误：未找到名为 ${CONTAINER_NAME} 的运行中容器。" | tee -a "$LOG_FILE"
    exit 1
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') - 开始监控容器 ${CONTAINER_NAME}" | tee -a "$LOG_FILE"

# 创建 CSV 文件头
echo "Timestamp,CPU_Percent,Memory_Percent,Memory_Usage,Memory_Limit" > "$CSV_FILE"

# 监控循环
while true; do
    # 使用 docker stats 获取瞬时资源使用情况（不带流式输出）
    STATS_OUTPUT=$(docker stats --no-stream --format "{{.CPUPerc}},{{.MemPerc}},{{.MemUsage}}" "${CONTAINER_NAME}" 2>/dev/null)

    if [ -n "$STATS_OUTPUT" ]; then
        # 提取数据并去除百分号
        CPU_USAGE=$(echo "$STATS_OUTPUT" | cut -d',' -f1 | sed 's/%//')
        MEM_USAGE=$(echo "$STATS_OUTPUT" | cut -d',' -f2 | sed 's/%//')
        MEM_DETAILS=$(echo "$STATS_OUTPUT" | cut -d',' -f3)

        # 当前时间戳
        CURRENT_TIME=$(date '+%Y-%m-%d %H:%M:%S')

        # 记录到日志文件
        LOG_ENTRY="${CURRENT_TIME} - CPU: ${CPU_USAGE}% - MEM: ${MEM_USAGE}% - 内存详情: ${MEM_DETAILS}"
        echo "$LOG_ENTRY" | tee -a "$LOG_FILE"

        # 记录到 CSV 文件
        echo "${CURRENT_TIME},${CPU_USAGE},${MEM_USAGE},${MEM_DETAILS}" >> "$CSV_FILE"

        # 更新统计数据
        SAMPLE_COUNT=$((SAMPLE_COUNT + 1))
        CPU_SUM=$(echo "$CPU_SUM + $CPU_USAGE" | bc)
        MEM_SUM=$(echo "$MEM_SUM + $MEM_USAGE" | bc)
        CPU_MAX=$(max "$CPU_MAX" "$CPU_USAGE")
        MEM_MAX=$(max "$MEM_MAX" "$MEM_USAGE")
        CPU_MIN=$(min "$CPU_MIN" "$CPU_USAGE")
        MEM_MIN=$(min "$MEM_MIN" "$MEM_USAGE")

    else
        ERROR_MSG="$(date '+%Y-%m-%d %H:%M:%S') - 警告：无法获取容器 ${CONTAINER_NAME} 的统计数据"
        echo "$ERROR_MSG" | tee -a "$LOG_FILE"

        # 检查容器是否还在运行
        if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
            echo "$(date '+%Y-%m-%d %H:%M:%S') - 容器已停止，结束监控。" | tee -a "$LOG_FILE"
            generate_stats
            exit 0
        fi
    fi

    # 等待指定的间隔时间
    sleep "$SLEEP_INTERVAL"
done
