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
ISOLATED模式Consumer Agent Worker - 独立进程实现 (Pulsar版本)
每个Consumer Agent作为独立进程运行，完全隔离
"""

import logging
import time


def isolated_pulsar_consumer_agent(agent_id, topic, subscription_name, pulsar_client_config, pulsar_consumer_config,
                                   stop_event, stats_queue, reset_flag, ready_queue):
    """
    独立Consumer Agent进程工作函数 - ISOLATED模式 (Pulsar版本)

    :param agent_id: Agent唯一ID
    :param topic: 要订阅的主题
    :param subscription_name: Subscription name
    :param pulsar_client_config: Pulsar Client配置字典
    :param pulsar_consumer_config: Pulsar Consumer配置字典
    :param stop_event: multiprocessing.Event - 停止信号
    :param stats_queue: multiprocessing.Queue - 统计数据队列
    :param reset_flag: multiprocessing.Value - 重置标志（epoch计数器）
    :param ready_queue: multiprocessing.Queue - 就绪/错误信号队列
    """
    # 设置进程级日志
    logger = logging.getLogger(f"pulsar-consumer-agent-{agent_id}")
    logger.setLevel(logging.INFO)

    try:
        # 通知主进程：Agent正在初始化
        logger.info(f"Pulsar Consumer Agent {agent_id} initializing...")

        # 在进程内导入（避免序列化问题）
        import pulsar
        from hdrh.histogram import HdrHistogram

        logger.info(f"Pulsar Consumer Agent {agent_id} starting (independent consumer process)")

        # 1. 创建独立的Pulsar Client
        client = pulsar.Client(
            service_url=pulsar_client_config.get('serviceUrl', 'pulsar://localhost:6650'),
            io_threads=pulsar_client_config.get('ioThreads', 1),
            operation_timeout_seconds=pulsar_client_config.get('operationTimeoutSeconds', 30),
        )

        # 2. 解析订阅类型
        subscription_type_str = pulsar_consumer_config.get('subscriptionType', 'Failover')
        subscription_type_map = {
            'Exclusive': pulsar.ConsumerType.Exclusive,
            'Shared': pulsar.ConsumerType.Shared,
            'Failover': pulsar.ConsumerType.Failover,
            'Key_Shared': pulsar.ConsumerType.KeyShared,
        }
        subscription_type = subscription_type_map.get(subscription_type_str, pulsar.ConsumerType.Failover)

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
        message_count = [0]  # Use list to allow modification in nested function

        # 4. 定义 MessageListener 回调函数（与 Java 版本一致）
        def message_listener(consumer, msg):
            """Message listener callback - 异步接收消息（与Java版本一致）"""
            try:
                message_count[0] += 1
                local_stats.messages_received += 1

                # 从 payload 中提取时间戳（前8字节）
                payload = msg.data()
                if payload and len(payload) >= 8:
                    import struct
                    # 解析前8字节的时间戳（大端序）
                    publish_timestamp_ms = struct.unpack('>Q', payload[:8])[0]
                    receive_timestamp_ms = int(time.time() * 1000)
                    e2e_latency_ms = receive_timestamp_ms - publish_timestamp_ms

                    # 统计完整消息大小（包含时间戳），与Producer保持一致
                    local_stats.bytes_received += len(payload)

                    if message_count[0] <= 5:
                        logger.info(f"Pulsar Consumer Agent {agent_id} msg {message_count[0]}: E2E latency={e2e_latency_ms} ms")

                    # 记录到histogram（与Java版本一致）
                    local_stats.record_e2e_latency(e2e_latency_ms)
                else:
                    # Payload 太小或为空，无法提取时间戳
                    local_stats.bytes_received += len(payload) if payload else 0
                    if message_count[0] <= 5:
                        logger.warning(f"Pulsar Consumer Agent {agent_id} msg {message_count[0]}: Payload too small")

                # Acknowledge消息（异步）
                consumer.acknowledge(msg)

            except Exception as e:
                logger.error(f"Pulsar Consumer Agent {agent_id} error in message_listener: {e}", exc_info=True)

        # 5. 创建Consumer with MessageListener（与Java版本一致）
        consumer = client.subscribe(
            topic=topic,
            subscription_name=subscription_name,
            consumer_type=subscription_type,
            receiver_queue_size=pulsar_consumer_config.get('receiverQueueSize', 1000),
            message_listener=message_listener  # ✅ 使用异步 MessageListener
        )
        logger.info(f"Pulsar Consumer Agent {agent_id} subscribed with MessageListener to topic: {topic}, subscription: {subscription_name}, type: {subscription_type_str}")

        # 发送就绪信号给主进程
        try:
            ready_queue.put({'agent_id': agent_id, 'status': 'ready', 'type': 'consumer'}, timeout=1.0)
            logger.info(f"Pulsar Consumer Agent {agent_id} is ready")
        except Exception as e:
            logger.error(f"Pulsar Consumer Agent {agent_id} failed to send ready signal: {e}")

        # 6. 统计汇报时间和epoch跟踪
        last_stats_report = time.time()
        last_epoch_check = time.time()
        current_epoch = reset_flag.value if reset_flag else 0

        # 7. Consumer主循环（MessageListener模式：不需要主动receive）
        # MessageListener会在后台线程异步接收消息，主循环只需要汇报统计和检查停止信号
        while not stop_event.is_set():
            try:
                # 7.1 等待一小段时间（MessageListener在后台接收消息）
                time.sleep(0.1)

                # 7.2 检查epoch重置（每秒检查一次）
                now = time.time()
                if now - last_epoch_check > 1.0:
                    if reset_flag:
                        new_epoch = reset_flag.value
                        if new_epoch > current_epoch:
                            logger.info(f"Pulsar Consumer Agent {agent_id} detected stats reset: epoch {current_epoch} -> {new_epoch}")
                            current_epoch = new_epoch
                            # 重置本地统计
                            local_stats.messages_received = 0
                            local_stats.bytes_received = 0
                            local_stats.reset_histogram()

                    last_epoch_check = now

                # 7.3 定期汇报统计（每秒一次）
                if now - last_stats_report >= 1.0:
                    try:
                        # 发送histogram编码数据（与Java版本一致）
                        stats_dict = {
                            'agent_id': agent_id,
                            'type': 'consumer',
                            'messages_received': local_stats.messages_received,
                            'bytes_received': local_stats.bytes_received,
                            'timestamp': now,
                            'epoch': current_epoch,
                            # 发送编码后的histogram
                            'e2e_latency_histogram_encoded': local_stats.e2e_latency_histogram.encode(),
                        }

                        # 使用带超时的put，避免队列满时阻塞
                        queue_put_success = False
                        try:
                            stats_queue.put(stats_dict, timeout=0.1)
                            queue_put_success = True
                        except:
                            logger.warning(f"Pulsar Consumer Agent {agent_id} stats queue full, dropping stats")

                        # 重置周期统计
                        local_stats.messages_received = 0
                        local_stats.bytes_received = 0
                        local_stats.reset_histogram()

                        if not queue_put_success:
                            logger.warning(f"Pulsar Consumer Agent {agent_id} cleared histogram after queue full")

                    except Exception as e:
                        logger.error(f"Pulsar Consumer Agent {agent_id} failed to send stats: {e}", exc_info=True)
                        local_stats.reset_histogram()

                    last_stats_report = now

            except KeyboardInterrupt:
                logger.info(f"Pulsar Consumer Agent {agent_id} received keyboard interrupt, stopping...")
                break
            except Exception as e:
                logger.error(f"Pulsar Consumer Agent {agent_id} error in receive loop: {e}", exc_info=True)
                time.sleep(0.1)

        logger.info(f"Pulsar Consumer Agent {agent_id} stopping gracefully (received {message_count[0]} messages total)")

    except Exception as e:
        logger.error(f"Pulsar Consumer Agent {agent_id} fatal error: {e}", exc_info=True)
        # 发送错误信号给主进程
        try:
            ready_queue.put({'agent_id': agent_id, 'status': 'error', 'type': 'consumer', 'error': str(e)}, timeout=0.5)
        except:
            pass

    finally:
        # 7. 清理资源
        try:
            consumer.close()
            logger.info(f"Pulsar Consumer Agent {agent_id} closed consumer")
        except Exception as e:
            logger.error(f"Pulsar Consumer Agent {agent_id} error closing consumer: {e}")

        try:
            client.close()
            logger.info(f"Pulsar Consumer Agent {agent_id} closed client")
        except Exception as e:
            logger.error(f"Pulsar Consumer Agent {agent_id} error closing client: {e}")

        # 8. 发送最终统计
        try:
            final_stats = {
                'agent_id': agent_id,
                'type': 'consumer',
                'final': True,
                'total_messages': message_count[0]
            }
            stats_queue.put(final_stats, timeout=1.0)
        except Exception as e:
            logger.error(f"Pulsar Consumer Agent {agent_id} error sending final stats: {e}")

        logger.info(f"Pulsar Consumer Agent {agent_id} terminated")
