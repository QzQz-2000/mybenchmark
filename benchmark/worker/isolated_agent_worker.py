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
ISOLATED模式Agent Worker - 独立进程实现
每个Agent代表一个独立的数字孪生实体（传感器、设备、车辆等）
"""

import logging
import time
import random


def isolated_agent_worker(agent_id, topic, kafka_producer_config, kafka_consumer_config,
                         work_assignment, stop_event, stats_queue, shared_rate, reset_flag, ready_queue,
                         start_producing_event=None):
    """
    独立Agent进程工作函数 - ISOLATED模式
    模拟一个完全独立的数字孪生实体

    :param agent_id: Agent唯一ID
    :param topic: 要发送/订阅的主题
    :param kafka_producer_config: Kafka Producer配置字典
    :param kafka_consumer_config: Kafka Consumer配置字典（可选）
    :param work_assignment: ProducerWorkAssignment对象（包含payload、rate等）
    :param stop_event: multiprocessing.Event - 停止信号
    :param stats_queue: multiprocessing.Queue - 统计数据队列
    :param shared_rate: multiprocessing.Value - 共享速率（支持动态调整）
    :param reset_flag: multiprocessing.Value - 重置标志（epoch计数器）
    :param ready_queue: multiprocessing.Queue - 就绪/错误信号队列
    """
    # 设置进程级日志
    logger = logging.getLogger(f"agent-{agent_id}")
    logger.setLevel(logging.INFO)

    try:
        # 通知主进程：Agent正在初始化
        logger.info(f"Agent {agent_id} initializing...")
        # 在进程内导入（避免序列化问题）
        from confluent_kafka import Producer, Consumer
        from hdrh.histogram import HdrHistogram
        from benchmark.utils.uniform_rate_limiter import UniformRateLimiter

        logger.info(f"Agent {agent_id} starting (simulating independent digital twin entity)")

        # 1. 创建独立的Kafka Producer（每个Agent自己的连接）
        producer_config = kafka_producer_config.copy()
        producer_config['client.id'] = f'agent-{agent_id}-producer'
        producer = Producer(producer_config)

        # 2. 本地统计对象（进程内独立）
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

        # 3. 获取payload
        if not work_assignment or not work_assignment.payload_data:
            logger.error(f"Agent {agent_id}: No payload data provided")
            return

        # 原始 payload（不包含时间戳）
        base_payload = work_assignment.payload_data[0] if work_assignment.payload_data else bytes(1024)

        # 4. 速率限制器
        current_rate = shared_rate.value if shared_rate else work_assignment.publish_rate
        rate_limiter = UniformRateLimiter(current_rate)

        logger.info(f"Agent {agent_id} configured: topic={topic}, base_payload_size={len(base_payload)}, rate={current_rate} msg/s")

        # 发送就绪信号给主进程
        try:
            ready_queue.put({'agent_id': agent_id, 'status': 'ready', 'type': 'producer'}, timeout=1.0)
            logger.info(f"Agent {agent_id} is ready")
        except Exception as e:
            logger.error(f"Agent {agent_id} failed to send ready signal: {e}")

        # ✅ 等待开始生产信号（让 Consumer Group 先稳定）
        if start_producing_event:
            logger.info(f"Agent {agent_id} waiting for start_producing signal...")
            start_producing_event.wait()  # 阻塞直到主进程设置此事件
            logger.info(f"Agent {agent_id} received start signal, beginning to produce messages")

        # 5. 统计汇报时间和epoch跟踪
        last_rate_check = time.time()
        last_stats_report = time.time()
        current_epoch = reset_flag.value if reset_flag else 0

        # 6. Agent主循环
        message_count = 0
        loop_times = []  # Track loop timing for debugging

        while not stop_event.is_set():
            try:
                # ✅ 检查是否应该暂停（用于 backlog 模式，不用于 warmup）
                if start_producing_event and not start_producing_event.is_set():
                    # Producer 被暂停，等待重新开始信号
                    producer.poll(0)  # 继续处理 callbacks
                    time.sleep(0.01)  # 短暂sleep避免CPU空转
                    continue

                loop_iter_start = time.perf_counter() if message_count < 50 else None

                # 6.1 检查速率调整和epoch重置（每100ms检查一次）
                now = time.time()
                if now - last_rate_check > 0.1:
                    # 检查速率调整
                    if shared_rate:
                        new_rate = shared_rate.value
                        if abs(new_rate - current_rate) > 0.01:
                            current_rate = new_rate
                            rate_limiter = UniformRateLimiter(current_rate)
                            logger.info(f"Agent {agent_id} rate adjusted to {current_rate:.2f} msg/s")

                    # 检查epoch重置
                    if reset_flag:
                        new_epoch = reset_flag.value
                        if new_epoch > current_epoch:
                            logger.info(f"Agent {agent_id} detected stats reset: epoch {current_epoch} -> {new_epoch}, resetting local stats")

                            # 🔧 FIX Bug #3: 先发送旧 epoch 的最终统计，再切换到新 epoch
                            # 这样可以避免最后一批 histogram 数据丢失
                            try:
                                final_old_epoch_stats = {
                                    'agent_id': agent_id,
                                    'type': 'producer',
                                    'messages_sent': local_stats.messages_sent,
                                    'bytes_sent': local_stats.bytes_sent,
                                    'errors': local_stats.errors,
                                    'timestamp': now,
                                    'epoch': current_epoch,
                                    'final_epoch': True,  # 标记为 epoch 的最后一批数据
                                    'pub_latency_histogram_encoded': local_stats.pub_latency_histogram.encode(),
                                    'pub_delay_histogram_encoded': local_stats.pub_delay_histogram.encode(),
                                }
                                stats_queue.put(final_old_epoch_stats, timeout=1.0)
                                logger.info(f"Agent {agent_id} sent final stats for epoch {current_epoch}")
                            except Exception as e:
                                logger.warning(f"Agent {agent_id} failed to send final epoch {current_epoch} stats: {e}")

                            # 切换到新 epoch
                            current_epoch = new_epoch

                            # 重置本地统计（histogram 在 epoch 切换时需要重置）
                            local_stats.messages_sent = 0
                            local_stats.bytes_sent = 0
                            local_stats.errors = 0
                            local_stats.reset_histograms()  # Epoch 切换时重置 histogram 是合理的

                            # 不重新创建RateLimiter，保持速率连续性（与Java版本一致）
                            # 立即发送新epoch统计，无需等待下一个报告周期
                            last_stats_report = now - 1.0  # 强制下次循环立即发送

                    last_rate_check = now

                # 6.2 速率限制 - 获取预期发送时间
                intended_send_time_ns = rate_limiter.acquire()

                # 6.3 等待并处理callbacks（避免callback堆积，同时保持精确的发送时间）
                # 策略：在等待期间持续处理callbacks（非阻塞），精确控制发送时间
                remaining_ns = intended_send_time_ns - time.perf_counter_ns()

                # 在等待期间持续poll(0)处理callbacks，直到接近发送时间
                while remaining_ns > 100_000:  # 剩余超过100微秒
                    producer.poll(0)  # 非阻塞poll，立即返回

                    # 重新计算剩余时间
                    remaining_ns = intended_send_time_ns - time.perf_counter_ns()

                    # 如果还有较多剩余时间，短暂sleep避免CPU空转
                    if remaining_ns > 500_000:  # 剩余超过500微秒
                        time.sleep(0.0001)  # sleep 100微秒

                # 最后精确等待到intended_send_time
                remaining_ns = intended_send_time_ns - time.perf_counter_ns()
                if remaining_ns > 0:
                    time.sleep(remaining_ns / 1_000_000_000)

                send_time_ns = time.perf_counter_ns()

                # 6.4 选择key（根据分布策略）
                key = None
                if work_assignment.key_distributor_type and hasattr(work_assignment.key_distributor_type, 'name'):
                    if work_assignment.key_distributor_type.name == 'RANDOM_NANO':
                        key = str(random.randint(0, 1000000))
                    elif work_assignment.key_distributor_type.name == 'KEY_ROUND_ROBIN':
                        key = str(agent_id)
                    # NO_KEY: key保持None

                # 6.5 在 payload 前面加上时间戳（8字节，大端序）
                import struct
                send_timestamp_ms = int(time.time() * 1000)
                payload_with_timestamp = struct.pack('>Q', send_timestamp_ms) + base_payload

                # 6.6 发送消息（异步）+ 轻量级回调统计
                def delivery_callback(err, msg, intended=intended_send_time_ns, sent=send_time_ns):
                    if err:
                        local_stats.errors += 1
                    else:
                        ack_time_ns = time.perf_counter_ns()
                        local_stats.messages_sent += 1
                        # 统计实际发送的字节数（包含8字节时间戳），与Java版本一致
                        # Java版本统计的是完整的消息大小（包含所有header和timestamp）
                        local_stats.bytes_sent += len(payload_with_timestamp)

                        # 计算延迟（毫秒）- 与Java版本一致
                        publish_latency_ms = (ack_time_ns - sent) // 1_000_000
                        publish_delay_ms = (sent - intended) // 1_000_000

                        # 记录到HdrHistogram（与Java版本一致，无样本数限制）
                        local_stats.record_pub_latency(publish_latency_ms)
                        local_stats.record_pub_delay(publish_delay_ms)

                producer.produce(
                    topic=topic,
                    key=key.encode('utf-8') if key else None,
                    value=payload_with_timestamp,
                    callback=delivery_callback
                )

                message_count += 1

                # 6.7 发送后再次poll(0)确保新消息开始处理
                # 前面的poll循环已经处理了大部分callbacks，这里只是触发新消息的处理
                producer.poll(0)

                # Log timing for first 50 iterations
                if loop_iter_start is not None:
                    loop_time = (time.perf_counter() - loop_iter_start) * 1000  # ms
                    logger.info(f"Agent {agent_id} iteration {message_count}: {loop_time:.2f} ms")

                # 6.6 定期汇报统计（每秒一次）
                if now - last_stats_report >= 1.0:
                    try:
                        # 🔧 FIX Bug #1: 只在成功发送后才重置 histogram，避免数据丢失
                        # 编码后的histogram非常紧凑（通常几KB）
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
                            # 队列满，记录警告但保留 histogram 数据到下次发送
                            logger.warning(f"Agent {agent_id} stats queue full, preserving histogram for next period (sent={local_stats.messages_sent})")

                        # ✅ 计数器总是重置（避免重复计数）
                        # 计数器是增量数据，不能累加；histogram 是累积数据，可以保留
                        local_stats.messages_sent = 0
                        local_stats.bytes_sent = 0
                        local_stats.errors = 0

                        # 🔧 FIX Bug #2: Histogram 改为累积语义（不重置），与 Consumer 保持一致
                        # Histogram 永远不重置，累积整个测试期间的所有样本
                        # 如果队列满导致发送失败，histogram 保留，下次会重新发送完整的累积数据
                        # 主进程会在每次 get_period_stats() 时合并所有 Agent 的 histogram
                        # 然后使用 Recorder 的双缓冲机制获取增量数据

                    except Exception as e:
                        logger.error(f"Agent {agent_id} failed to send stats: {e}", exc_info=True)
                        # ✅ 异常时也要重置计数器（避免重复计数）
                        # Histogram 保留，下次会重新发送完整数据
                        local_stats.messages_sent = 0
                        local_stats.bytes_sent = 0
                        local_stats.errors = 0

                    last_stats_report = now

            except KeyboardInterrupt:
                logger.info(f"Agent {agent_id} received keyboard interrupt, stopping...")
                break
            except Exception as e:
                logger.error(f"Agent {agent_id} error in send loop: {e}", exc_info=True)
                local_stats.errors += 1

                # 根据错误类型采取不同的恢复策略
                error_str = str(e).lower()
                if 'broker' in error_str or 'connection' in error_str or 'network' in error_str:
                    # 网络/连接错误 - 等待较长时间后重试
                    logger.warning(f"Agent {agent_id} detected connection error, backing off for 1s")
                    time.sleep(1.0)
                elif 'queue' in error_str or 'buffer' in error_str:
                    # 队列/缓冲区错误 - 等待中等时间
                    logger.warning(f"Agent {agent_id} detected queue/buffer error, backing off for 100ms")
                    time.sleep(0.1)
                else:
                    # 其他错误 - 短暂等待
                    time.sleep(0.01)

                # 连续错误检测：如果错误率过高，停止Agent
                if local_stats.errors > 100 and message_count > 0:
                    error_rate = local_stats.errors / message_count
                    if error_rate > 0.5:  # 错误率超过50%
                        logger.error(f"Agent {agent_id} error rate too high ({error_rate:.1%}), stopping")
                        break

        logger.info(f"Agent {agent_id} stopping gracefully (sent {message_count} messages total)")

    except Exception as e:
        logger.error(f"Agent {agent_id} fatal error: {e}", exc_info=True)
        # 发送错误信号给主进程
        try:
            ready_queue.put({'agent_id': agent_id, 'status': 'error', 'type': 'producer', 'error': str(e)}, timeout=0.5)
        except:
            pass

    finally:
        # 7. 清理资源
        try:
            # Flush所有待发送消息
            producer.flush(10)
            logger.info(f"Agent {agent_id} flushed pending messages")
        except Exception as e:
            logger.error(f"Agent {agent_id} error flushing producer: {e}")

        # 8. 发送最终统计
        try:
            final_stats = {
                'agent_id': agent_id,
                'final': True,
                'total_messages': message_count
            }
            stats_queue.put(final_stats, timeout=1.0)
        except Exception as e:
            logger.error(f"Agent {agent_id} error sending final stats: {e}")

        logger.info(f"Agent {agent_id} terminated")
