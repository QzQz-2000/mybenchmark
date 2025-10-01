#!/usr/bin/env python3
"""Debug Consumer - 直接测试 Consumer 是否能接收消息"""

import asyncio
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from benchmark.drivers.kafka import KafkaDriver
from benchmark.core.config import ConfigLoader


async def test_consumer():
    """测试 Consumer 是否能接收消息"""

    # 加载配置
    driver_config = ConfigLoader.load_driver("configs/kafka-digital-twin.yaml")

    # 创建 topic
    topic_name = "debug-test-topic"

    print(f"\n📋 Topic: {topic_name}")
    print("="*80)

    # 1. 创建 Driver 和 topic
    driver = KafkaDriver(driver_config)
    await driver.initialize()

    try:
        # 创建 topic
        print("🔧 Creating topic...")
        await driver.create_topic(topic_name, partitions=4)
        print("✅ Topic created")

        # 2. 创建 Producer 并发送一些消息
        print("\n📤 Sending 10 test messages...")
        producer = driver.create_producer()

        from benchmark.drivers.base import Message
        import time

        for i in range(10):
            msg = Message(
                key=None,
                value=f"Test message {i}".encode(),
                headers={"send_timestamp": str(time.time() * 1000).encode()},
                timestamp=time.time()
            )
            await producer.send_message(topic_name, msg)
            print(f"  Sent message {i}")

        await producer.flush()
        print("✅ All messages sent and flushed")

        # 3. 创建 Consumer 并消费
        print("\n📥 Starting consumer...")
        consumer = driver.create_consumer()

        group_name = "debug-consumer-group"
        await consumer.subscribe([topic_name], group_name)
        print(f"✅ Subscribed to {topic_name} with group {group_name}")

        # 4. 消费消息
        print("\n🔄 Consuming messages (10 seconds timeout)...")
        messages_received = 0
        start_time = time.time()

        while time.time() - start_time < 10:
            async for msg in consumer.consume_messages(timeout_seconds=1.0):
                messages_received += 1
                value = msg.message.value.decode() if msg.message.value else ""
                print(f"  ✅ Received: {value}")

        print(f"\n📊 Total messages received: {messages_received}")

        # Cleanup
        await consumer.close()
        await producer.close()
        await driver.delete_topic(topic_name)
        await driver.close()

        if messages_received > 0:
            print("\n✅ SUCCESS: Consumer is working correctly!")
            return True
        else:
            print("\n❌ FAILURE: Consumer did not receive any messages!")
            return False

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    result = asyncio.run(test_consumer())
    sys.exit(0 if result else 1)
