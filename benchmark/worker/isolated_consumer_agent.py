# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
ISOLATED模式Consumer Agent Worker - 独立进程实现
每个Consumer Agent作为独立进程运行，完全隔离
"""

import logging
import time


def isolated_consumer_agent(agent_id, topic, subscription_name, kafka_consumer_config,
                            stop_event, stats_queue, reset_flag, ready_queue):
    """
    独立Consumer Agent进程工作函数 - ISOLATED模式

    :param agent_id: Agent唯一ID
    :param topic: 要订阅的主题
    :param subscription_name: Consumer group name
    :param kafka_consumer_config: Kafka Consumer配置字典
    :param stop_event: multiprocessing.Event - 停止信号
    :param stats_queue: multiprocessing.Queue - 统计数据队列
    :param reset_flag: multiprocessing.Value - 重置标志（epoch计数器）
    :param ready_queue: multiprocessing.Queue - 就绪/错误信号队列
    """
    # 设置进程级日志
    logger = logging.getLogger(f"consumer-agent-{agent_id}")
    logger.setLevel(logging.INFO)

    try:
        # 通知主进程：Agent正在初始化
        logger.info(f"Consumer Agent {agent_id} initializing...")

        # 在进程内导入（避免序列化问题）
        from confluent_kafka import Consumer, KafkaError
        from hdrh.histogram import HdrHistogram

        logger.info(f"Consumer Agent {agent_id} starting (independent consumer process)")

        # 1. 创建独立的Kafka Consumer
        consumer_config = kafka_consumer_config.copy()
        consumer_config['group.id'] = subscription_name
        consumer_config['client.id'] = f'consumer-agent-{agent_id}'
        consumer = Consumer(consumer_config)

        # 2. 订阅topic
        consumer.subscribe([topic])
        logger.info(f"Consumer Agent {agent_id} subscribed to topic: {topic}, group: {subscription_name}")

        # 3. 本地统计对象（进程内独立）
        # 使用HdrHistogram替代list（与Java版本一致）
        class LocalStats:
            def __init__(self):
                self.messages_received = 0
                self.bytes_received = 0
                # End-to-end延迟histogram（范围更大：12小时）
                self.e2e_latency_histogram = HdrHistogram(1, 12 * 60 * 60 * 1_000, 5)

            def record_e2e_latency(self, latency_ms):
                """记录端到端延迟（毫秒）"""
                if 0 < latency_ms <= 12 * 60 * 60 * 1_000:
                    self.e2e_latency_histogram.record_value(int(latency_ms))

            def reset_histogram(self):
                """重置histogram"""
                self.e2e_latency_histogram = HdrHistogram(1, 12 * 60 * 60 * 1_000, 5)

        local_stats = LocalStats()

        # 发送就绪信号给主进程
        try:
            ready_queue.put({'agent_id': agent_id, 'status': 'ready', 'type': 'consumer'}, timeout=1.0)
            logger.info(f"Consumer Agent {agent_id} is ready")
        except Exception as e:
            logger.error(f"Consumer Agent {agent_id} failed to send ready signal: {e}")

        # 4. 统计汇报时间和epoch跟踪
        last_stats_report = time.time()
        last_epoch_check = time.time()
        current_epoch = reset_flag.value if reset_flag else 0

        # 4.5 文件输出配置（双输出模式：Queue + File）
        import os
        import pickle
        from pathlib import Path
        stats_dir = Path("/tmp/kafka_benchmark_stats")
        stats_dir.mkdir(parents=True, exist_ok=True)
        stats_file = stats_dir / f"consumer_{agent_id}_stats.pkl"
        last_file_write = time.time()
        file_write_interval = 5  # 每5秒写一次文件

        # 5. Consumer主循环
        message_count = 0

        while not stop_event.is_set():
            try:
                # 5.1 批量Poll消息（一次最多100条，timeout 1秒）
                messages = consumer.consume(num_messages=100, timeout=1.0)

                if not messages:
                    # 没有消息，继续
                    pass
                else:
                    # 批量处理消息
                    for msg in messages:
                        if msg.error():
                            # 错误处理
                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                # 分区末尾，正常情况
                                logger.debug(f"Consumer Agent {agent_id} reached end of partition {msg.partition()}")
                            else:
                                logger.error(f"Consumer Agent {agent_id} error: {msg.error()}")
                            continue

                        # 成功接收消息
                        message_count += 1
                        local_stats.messages_received += 1

                        # 从 payload 中提取时间戳（前8字节）
                        payload = msg.value()
                        if payload and len(payload) >= 8:
                            import struct
                            # 解析前8字节的时间戳（大端序）
                            publish_timestamp_ms = struct.unpack('>Q', payload[:8])[0]
                            receive_timestamp_ms = int(time.time() * 1000)
                            e2e_latency_ms = receive_timestamp_ms - publish_timestamp_ms

                            # 统计完整消息大小（包含时间戳），与Producer保持一致
                            # 这样Producer发送的bytes_sent和Consumer接收的bytes_received能对应上
                            local_stats.bytes_received += len(payload)

                            if message_count <= 5:
                                logger.info(f"Consumer Agent {agent_id} msg {message_count}: E2E latency={e2e_latency_ms} ms (pub={publish_timestamp_ms}, recv={receive_timestamp_ms})")

                            # 记录到histogram（与Java版本一致）
                            local_stats.record_e2e_latency(e2e_latency_ms)
                        else:
                            # Payload 太小或为空，无法提取时间戳
                            local_stats.bytes_received += len(payload) if payload else 0
                            if message_count <= 5:
                                logger.warning(f"Consumer Agent {agent_id} msg {message_count}: Payload too small ({len(payload) if payload else 0} bytes), cannot extract timestamp")

                # 5.2 检查epoch重置（每秒检查一次）
                now = time.time()
                if now - last_epoch_check > 1.0:
                    if reset_flag:
                        new_epoch = reset_flag.value
                        if new_epoch > current_epoch:
                            logger.info(f"Consumer Agent {agent_id} detected stats reset: epoch {current_epoch} -> {new_epoch}")
                            current_epoch = new_epoch
                            # 重置本地统计
                            local_stats.messages_received = 0
                            local_stats.bytes_received = 0
                            local_stats.reset_histogram()

                    last_epoch_check = now

                # 5.3 定期汇报统计（每秒一次）- 已注释，仅使用文件输出
                # if now - last_stats_report >= 1.0:
                #     try:
                #         # 发送histogram编码数据（与Java版本一致）
                #         # 注意：只发送周期计数器，histogram不重置（累积统计）
                #         stats_dict = {
                #             'agent_id': agent_id,
                #             'type': 'consumer',
                #             'messages_received': local_stats.messages_received,
                #             'bytes_received': local_stats.bytes_received,
                #             'timestamp': now,
                #             'epoch': current_epoch,
                #             # 发送编码后的histogram（累积数据，不重置）
                #             'e2e_latency_histogram_encoded': local_stats.e2e_latency_histogram.encode(),
                #         }
                #
                #         # 使用带超时的put，避免队列满时阻塞
                #         try:
                #             stats_queue.put(stats_dict, timeout=0.1)
                #             # 只重置周期计数器，不重置histogram（histogram是累积的）
                #             local_stats.messages_received = 0
                #             local_stats.bytes_received = 0
                #         except:
                #             logger.warning(f"Consumer Agent {agent_id} stats queue full, dropping stats report")
                #             # 即使队列满，也重置计数器避免重复计数
                #             local_stats.messages_received = 0
                #             local_stats.bytes_received = 0
                #
                #     except Exception as e:
                #         logger.error(f"Consumer Agent {agent_id} failed to send stats: {e}", exc_info=True)
                #
                #     last_stats_report = now

                # 5.4 定期写入文件（每5秒一次）
                if now - last_file_write >= file_write_interval:
                    try:
                        # 写入累积统计到文件
                        file_data = {
                            'agent_id': agent_id,
                            'total_messages_received': message_count,
                            'e2e_latency_histogram_encoded': local_stats.e2e_latency_histogram.encode(),
                            'timestamp': now,
                            'epoch': current_epoch
                        }

                        # 原子写入
                        temp_file = stats_file.with_suffix('.tmp')
                        with open(temp_file, 'wb') as f:
                            pickle.dump(file_data, f)
                        temp_file.rename(stats_file)

                        logger.debug(f"Consumer Agent {agent_id} wrote stats to {stats_file}")
                        last_file_write = now
                    except Exception as e:
                        logger.error(f"Consumer Agent {agent_id} failed to write stats file: {e}", exc_info=True)

            except KeyboardInterrupt:
                logger.info(f"Consumer Agent {agent_id} received keyboard interrupt, stopping...")
                break
            except Exception as e:
                logger.error(f"Consumer Agent {agent_id} error in poll loop: {e}", exc_info=True)
                time.sleep(0.1)

        logger.info(f"Consumer Agent {agent_id} stopping gracefully (received {message_count} messages total)")

    except Exception as e:
        logger.error(f"Consumer Agent {agent_id} fatal error: {e}", exc_info=True)
        # 发送错误信号给主进程
        try:
            ready_queue.put({'agent_id': agent_id, 'status': 'error', 'type': 'consumer', 'error': str(e)}, timeout=0.5)
        except:
            pass

    finally:
        # 6. 清理资源
        try:
            consumer.close()
            logger.info(f"Consumer Agent {agent_id} closed consumer")
        except Exception as e:
            logger.error(f"Consumer Agent {agent_id} error closing consumer: {e}")

        # 7. 写入最终统计到文件
        try:
            final_file_data = {
                'agent_id': agent_id,
                'total_messages_received': message_count,
                'e2e_latency_histogram_encoded': local_stats.e2e_latency_histogram.encode(),
                'timestamp': time.time(),
                'epoch': current_epoch,
                'final': True
            }
            with open(stats_file, 'wb') as f:
                pickle.dump(final_file_data, f)
            logger.info(f"Consumer Agent {agent_id} wrote final stats to {stats_file}")
        except Exception as e:
            logger.error(f"Consumer Agent {agent_id} error writing final stats file: {e}")

        # 8. 发送最终统计到Queue - 已注释，仅使用文件输出
        # try:
        #     final_stats = {
        #         'agent_id': agent_id,
        #         'type': 'consumer',
        #         'final': True,
        #         'total_messages': message_count
        #     }
        #     stats_queue.put(final_stats, timeout=1.0)
        # except Exception as e:
        #     logger.error(f"Consumer Agent {agent_id} error sending final stats: {e}")

        logger.info(f"Consumer Agent {agent_id} terminated")
