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
ISOLATED模式Agent Worker - 独立进程实现 (Pulsar版本)
每个Agent代表一个独立的数字孪生实体（传感器、设备、车辆等）
"""

import logging
import time
import random


def isolated_pulsar_agent_worker(agent_id, topic, pulsar_client_config, pulsar_producer_config,
                                 work_assignment, stop_event, stats_queue, shared_rate, reset_flag, ready_queue):
    """
    独立Agent进程工作函数 - ISOLATED模式 (Pulsar版本)
    模拟一个完全独立的数字孪生实体

    :param agent_id: Agent唯一ID
    :param topic: 要发送/订阅的主题
    :param pulsar_client_config: Pulsar Client配置字典
    :param pulsar_producer_config: Pulsar Producer配置字典
    :param work_assignment: ProducerWorkAssignment对象（包含payload、rate等）
    :param stop_event: multiprocessing.Event - 停止信号
    :param stats_queue: multiprocessing.Queue - 统计数据队列
    :param shared_rate: multiprocessing.Value - 共享速率（支持动态调整）
    :param reset_flag: multiprocessing.Value - 重置标志（epoch计数器）
    :param ready_queue: multiprocessing.Queue - 就绪/错误信号队列
    """
    # 设置进程级日志
    logger = logging.getLogger(f"pulsar-agent-{agent_id}")
    logger.setLevel(logging.INFO)

    try:
        # 通知主进程：Agent正在初始化
        logger.info(f"Pulsar Agent {agent_id} initializing...")
        # 在进程内导入（避免序列化问题）
        import pulsar
        from hdrh.histogram import HdrHistogram
        from benchmark.utils.uniform_rate_limiter import UniformRateLimiter

        logger.info(f"Pulsar Agent {agent_id} starting (simulating independent digital twin entity)")

        # 1. 创建独立的Pulsar Client和Producer（每个Agent自己的连接）
        client = pulsar.Client(
            service_url=pulsar_client_config.get('serviceUrl', 'pulsar://localhost:6650'),
            io_threads=pulsar_client_config.get('ioThreads', 1),
            operation_timeout_seconds=pulsar_client_config.get('operationTimeoutSeconds', 30),
        )

        # 2. 创建Producer
        producer_config = {
            'topic': topic,
            'batching_enabled': pulsar_producer_config.get('batchingEnabled', True),
            'batching_max_publish_delay_ms': pulsar_producer_config.get('batchingMaxPublishDelayMs', 1),
            # 'batching_max_bytes': pulsar_producer_config.get('batchingMaxBytes', 131072),
            'block_if_queue_full': pulsar_producer_config.get('blockIfQueueFull', True),
            'max_pending_messages': pulsar_producer_config.get('pendingQueueSize', 1000),
        }

        producer = client.create_producer(**producer_config)

        # 3. 本地统计对象（进程内独立）
        # 使用HdrHistogram替代list，内存固定且精确（与Java版本一致）
        class LocalStats:
            def __init__(self):
                self.messages_sent = 0
                self.bytes_sent = 0
                self.errors = 0
                # 使用HdrHistogram记录延迟（与Java版本一致）
                # 延迟范围: 1ms - 60秒，精度5位有效数字
                self.pub_latency_histogram = HdrHistogram(1, 60 * 1_000, 5)
                self.pub_delay_histogram = HdrHistogram(1, 60 * 1_000, 5)

            def record_pub_latency(self, latency_ms):
                """记录发布延迟（毫秒）"""
                if 0 < latency_ms <= 60 * 1_000:
                    self.pub_latency_histogram.record_value(int(latency_ms))

            def record_pub_delay(self, delay_ms):
                """记录发布延迟（毫秒）"""
                if 0 <= delay_ms <= 60 * 1_000:
                    self.pub_delay_histogram.record_value(int(delay_ms))

            def reset_histograms(self):
                """重置histogram"""
                self.pub_latency_histogram = HdrHistogram(1, 60 * 1_000, 5)
                self.pub_delay_histogram = HdrHistogram(1, 60 * 1_000, 5)

        local_stats = LocalStats()

        # 4. 获取payload
        if not work_assignment or not work_assignment.payload_data:
            logger.error(f"Pulsar Agent {agent_id}: No payload data provided")
            return

        # 原始 payload（不包含时间戳）
        base_payload = work_assignment.payload_data[0] if work_assignment.payload_data else bytes(1024)

        # 5. 速率限制器
        current_rate = shared_rate.value if shared_rate else work_assignment.publish_rate
        rate_limiter = UniformRateLimiter(current_rate)

        logger.info(f"Pulsar Agent {agent_id} configured: topic={topic}, base_payload_size={len(base_payload)}, rate={current_rate} msg/s")

        # 发送就绪信号给主进程
        try:
            ready_queue.put({'agent_id': agent_id, 'status': 'ready', 'type': 'producer'}, timeout=1.0)
            logger.info(f"Pulsar Agent {agent_id} is ready")
        except Exception as e:
            logger.error(f"Pulsar Agent {agent_id} failed to send ready signal: {e}")

        # 6. 统计汇报时间和epoch跟踪
        last_rate_check = time.time()
        last_stats_report = time.time()
        current_epoch = reset_flag.value if reset_flag else 0

        # 7. Agent主循环
        message_count = 0
        pending_futures = []  # Track pending send operations

        while not stop_event.is_set():
            try:
                # 7.1 检查速率调整和epoch重置（每100ms检查一次）
                now = time.time()
                if now - last_rate_check > 0.1:
                    # 检查速率调整
                    if shared_rate:
                        new_rate = shared_rate.value
                        if abs(new_rate - current_rate) > 0.01:
                            current_rate = new_rate
                            rate_limiter = UniformRateLimiter(current_rate)
                            logger.info(f"Pulsar Agent {agent_id} rate adjusted to {current_rate:.2f} msg/s")

                    # 检查epoch重置
                    if reset_flag:
                        new_epoch = reset_flag.value
                        if new_epoch > current_epoch:
                            logger.info(f"Pulsar Agent {agent_id} detected stats reset: epoch {current_epoch} -> {new_epoch}, resetting local stats")
                            current_epoch = new_epoch
                            # 重置本地统计
                            local_stats.messages_sent = 0
                            local_stats.bytes_sent = 0
                            local_stats.errors = 0
                            local_stats.reset_histograms()

                    last_rate_check = now

                # 7.2 速率限制 - 获取预期发送时间
                intended_send_time_ns = rate_limiter.acquire()

                # 等待到发送时间
                remaining_ns = intended_send_time_ns - time.perf_counter_ns()
                if remaining_ns > 0:
                    time.sleep(remaining_ns / 1_000_000_000)

                send_time_ns = time.perf_counter_ns()

                # 7.3 选择key（根据分布策略）
                key = None
                if work_assignment.key_distributor_type and hasattr(work_assignment.key_distributor_type, 'name'):
                    if work_assignment.key_distributor_type.name == 'RANDOM_NANO':
                        key = str(random.randint(0, 1000000))
                    elif work_assignment.key_distributor_type.name == 'KEY_ROUND_ROBIN':
                        key = str(agent_id)
                    # NO_KEY: key保持None

                # 7.4 在 payload 前面加上时间戳（8字节，大端序）
                import struct
                send_timestamp_ms = int(time.time() * 1000)
                payload_with_timestamp = struct.pack('>Q', send_timestamp_ms) + base_payload

                # 7.5 发送消息（异步）
                try:
                    # Pulsar async send with callback
                    def send_callback(res, msg_id, intended=intended_send_time_ns, sent=send_time_ns, payload_size=len(payload_with_timestamp)):
                        if res == pulsar.Result.Ok:
                            ack_time_ns = time.perf_counter_ns()
                            local_stats.messages_sent += 1
                            local_stats.bytes_sent += payload_size

                            # 计算延迟（毫秒）
                            publish_latency_ms = (ack_time_ns - sent) // 1_000_000
                            publish_delay_ms = (sent - intended) // 1_000_000

                            # 记录到HdrHistogram
                            local_stats.record_pub_latency(publish_latency_ms)
                            local_stats.record_pub_delay(publish_delay_ms)
                        else:
                            local_stats.errors += 1
                            logger.error(f"Pulsar Agent {agent_id} send failed: {res}")

                    if key:
                        producer.send_async(
                            content=payload_with_timestamp,
                            partition_key=key,
                            callback=send_callback
                        )
                    else:
                        producer.send_async(
                            content=payload_with_timestamp,
                            callback=send_callback
                        )

                    message_count += 1

                except Exception as e:
                    logger.error(f"Pulsar Agent {agent_id} error sending message: {e}")
                    local_stats.errors += 1

                # 7.6 定期汇报统计（每秒一次）
                if now - last_stats_report >= 1.0:
                    try:
                        # 发送histogram编码数据（与Java版本一致）
                        stats_dict = {
                            'agent_id': agent_id,
                            'type': 'producer',
                            'messages_sent': local_stats.messages_sent,
                            'bytes_sent': local_stats.bytes_sent,
                            'errors': local_stats.errors,
                            'timestamp': now,
                            'epoch': current_epoch,
                            # 发送编码后的histogram（紧凑且完整）
                            'pub_latency_histogram_encoded': local_stats.pub_latency_histogram.encode(),
                            'pub_delay_histogram_encoded': local_stats.pub_delay_histogram.encode(),
                        }
                        # 使用带超时的put，避免队列满时阻塞
                        queue_put_success = False
                        try:
                            stats_queue.put(stats_dict, timeout=0.1)
                            queue_put_success = True
                        except:
                            # 队列满，记录警告并丢弃本次统计
                            logger.warning(f"Pulsar Agent {agent_id} stats queue full, dropping stats (sent={local_stats.messages_sent})")

                        # 重置周期统计（无论是否发送成功都要清空）
                        local_stats.messages_sent = 0
                        local_stats.bytes_sent = 0
                        local_stats.errors = 0
                        # 重置histogram（创建新实例）
                        local_stats.reset_histograms()

                        if not queue_put_success:
                            logger.warning(f"Pulsar Agent {agent_id} cleared histograms after queue full")

                    except Exception as e:
                        logger.error(f"Pulsar Agent {agent_id} failed to send stats: {e}", exc_info=True)
                        # 发生异常时也要重置histogram
                        local_stats.reset_histograms()

                    last_stats_report = now

            except KeyboardInterrupt:
                logger.info(f"Pulsar Agent {agent_id} received keyboard interrupt, stopping...")
                break
            except Exception as e:
                logger.error(f"Pulsar Agent {agent_id} error in send loop: {e}", exc_info=True)
                local_stats.errors += 1
                time.sleep(0.01)

                # 连续错误检测
                if local_stats.errors > 100 and message_count > 0:
                    error_rate = local_stats.errors / message_count
                    if error_rate > 0.5:
                        logger.error(f"Pulsar Agent {agent_id} error rate too high ({error_rate:.1%}), stopping")
                        break

        logger.info(f"Pulsar Agent {agent_id} stopping gracefully (sent {message_count} messages total)")

    except Exception as e:
        logger.error(f"Pulsar Agent {agent_id} fatal error: {e}", exc_info=True)
        # 发送错误信号给主进程
        try:
            ready_queue.put({'agent_id': agent_id, 'status': 'error', 'type': 'producer', 'error': str(e)}, timeout=0.5)
        except:
            pass

    finally:
        # 8. 清理资源
        try:
            # Flush所有待发送消息
            producer.flush()
            producer.close()
            logger.info(f"Pulsar Agent {agent_id} flushed and closed producer")
        except Exception as e:
            logger.error(f"Pulsar Agent {agent_id} error closing producer: {e}")

        try:
            client.close()
            logger.info(f"Pulsar Agent {agent_id} closed client")
        except Exception as e:
            logger.error(f"Pulsar Agent {agent_id} error closing client: {e}")

        # 9. 发送最终统计
        try:
            final_stats = {
                'agent_id': agent_id,
                'final': True,
                'total_messages': message_count
            }
            stats_queue.put(final_stats, timeout=1.0)
        except Exception as e:
            logger.error(f"Pulsar Agent {agent_id} error sending final stats: {e}")

        logger.info(f"Pulsar Agent {agent_id} terminated")
