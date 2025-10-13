#!/bin/bash
# 重命名控制变量实验的结果文件

echo "重命名结果文件..."

# 找到最新的5个test-workload文件
FILES=$(ls -t msg-size-*-controlled-Kafka-*.json 2>/dev/null || ls -t test-workload-Kafka-*.json 2>/dev/null | head -5)

if [ -z "$FILES" ]; then
    echo "❌ 未找到结果文件"
    exit 1
fi

for f in $FILES; do
    # 提取消息大小
    SIZE=$(grep -o '"messageSize": [0-9]*' "$f" | head -1 | cut -d' ' -f2)

    if [ -n "$SIZE" ]; then
        TIMESTAMP=$(echo $f | grep -o '[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}-[0-9]\{2\}-[0-9]\{2\}-[0-9]\{2\}')
        NEW_NAME="msg-size-${SIZE}B-controlled-Kafka-${TIMESTAMP}.json"

        if [ "$f" != "$NEW_NAME" ]; then
            cp "$f" "$NEW_NAME"
            echo "✅ $f -> $NEW_NAME"
        fi
    fi
done

echo ""
echo "重命名完成！现在运行分析："
echo "  python analyze_message_size_results.py"
