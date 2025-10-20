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
HTTP handler for benchmark worker.
Sets up REST API endpoints for worker control.
"""

import logging
from flask import Flask, request, jsonify
from .local_worker import LocalWorker

logger = logging.getLogger(__name__)


class WorkerHandler:
    """
    Worker HTTP handler.
    Sets up REST API endpoints for controlling the benchmark worker.
    """

    def __init__(self, app: Flask, stats_logger=None):
        """
        Initialize worker handler.

        :param app: Flask application
        :param stats_logger: Optional stats logger
        """
        self.app = app
        self.worker = LocalWorker(stats_logger)

        # Register routes
        self._register_routes()

    def _register_routes(self):
        """Register all HTTP routes."""

        @self.app.route('/initialize-driver', methods=['POST'])
        def initialize_driver():
            """Initialize driver endpoint."""
            file = request.files.get('file')
            if not file:
                return jsonify({'error': 'No file provided'}), 400

            # Save uploaded file temporarily
            temp_path = '/tmp/driver_config.yaml'
            file.save(temp_path)

            try:
                self.worker.initialize_driver(temp_path)
                return jsonify({'status': 'ok'})
            except Exception as e:
                logger.error(f"Error initializing driver: {e}", exc_info=True)
                import traceback
                return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500

        @self.app.route('/create-topics', methods=['POST'])
        def create_topics():
            """Create topics endpoint."""
            from .commands.topics_info import TopicsInfo

            data = request.json
            topics_info = TopicsInfo(
                number_of_topics=data['numberOfTopics'],
                number_of_partitions_per_topic=data['numberOfPartitionsPerTopic']
            )

            try:
                topics = self.worker.create_topics(topics_info)
                return jsonify(topics)
            except Exception as e:
                logger.error(f"Error creating topics: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/delete-topics', methods=['POST'])
        def delete_topics():
            """Delete topics endpoint."""
            data = request.json
            topics = data.get('topics', [])

            try:
                if hasattr(self.worker, 'benchmark_driver') and self.worker.benchmark_driver:
                    if hasattr(self.worker.benchmark_driver, 'delete_topics'):
                        delete_future = self.worker.benchmark_driver.delete_topics(topics)
                        delete_future.result()  # Wait for completion
                        return jsonify({'status': 'ok', 'deleted': len(topics)})
                    else:
                        return jsonify({'error': 'Driver does not support delete_topics'}), 400
                else:
                    return jsonify({'error': 'No benchmark driver initialized'}), 400
            except Exception as e:
                logger.error(f"Error deleting topics: {e}", exc_info=True)
                return jsonify({'error': str(e)}), 500

        @self.app.route('/create-producers', methods=['POST'])
        def create_producers():
            """Create producers endpoint."""
            topics = request.json
            try:
                self.worker.create_producers(topics)
                return jsonify({'status': 'ok'})
            except Exception as e:
                logger.error(f"Error creating producers: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/create-consumers', methods=['POST'])
        def create_consumers():
            """Create consumers endpoint."""
            from .commands.consumer_assignment import ConsumerAssignment
            from .commands.topic_subscription import TopicSubscription

            data = request.json
            assignment = ConsumerAssignment()
            assignment.topics_subscriptions = [
                TopicSubscription(ts['topic'], ts['subscription'])
                for ts in data['topicsSubscriptions']
            ]

            try:
                self.worker.create_consumers(assignment)
                return jsonify({'status': 'ok'})
            except Exception as e:
                logger.error(f"Error creating consumers: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/probe-producers', methods=['POST'])
        def probe_producers():
            """Probe producers endpoint."""
            try:
                self.worker.probe_producers()
                return jsonify({'status': 'ok'})
            except Exception as e:
                logger.error(f"Error probing producers: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/start-load', methods=['POST'])
        def start_load():
            """Start load endpoint."""
            from .commands.producer_work_assignment import ProducerWorkAssignment
            from benchmark.utils.distributor.key_distributor_type import KeyDistributorType

            data = request.json
            assignment = ProducerWorkAssignment()
            assignment.publish_rate = data['publishRate']
            message_processing_delay_ms = data.get('messageProcessingDelayMs', 0)

            # Set key distributor type
            if data.get('keyDistributorType'):
                assignment.key_distributor_type = KeyDistributorType(data['keyDistributorType'])

            # Set payload data (convert from list back to bytes)
            if data.get('payloadData'):
                assignment.payload_data = [bytes(payload) for payload in data['payloadData']]
            else:
                assignment.payload_data = []

            try:
                self.worker.start_load(assignment, message_processing_delay_ms)
                return jsonify({'status': 'ok'})
            except Exception as e:
                logger.error(f"Error starting load: {e}", exc_info=True)
                return jsonify({'error': str(e)}), 500

        @self.app.route('/adjust-rate', methods=['POST'])
        def adjust_rate():
            """Adjust rate endpoint."""
            data = request.json
            try:
                self.worker.adjust_publish_rate(data['publishRate'])
                return jsonify({'status': 'ok'})
            except Exception as e:
                logger.error(f"Error adjusting rate: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/pause-consumers', methods=['POST'])
        def pause_consumers():
            """Pause consumers endpoint."""
            try:
                self.worker.pause_consumers()
                return jsonify({'status': 'ok'})
            except Exception as e:
                logger.error(f"Error pausing consumers: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/resume-consumers', methods=['POST'])
        def resume_consumers():
            """Resume consumers endpoint."""
            try:
                self.worker.resume_consumers()
                return jsonify({'status': 'ok'})
            except Exception as e:
                logger.error(f"Error resuming consumers: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/counters-stats', methods=['GET'])
        def get_counters_stats():
            """Get counters stats endpoint."""
            try:
                stats = self.worker.get_counters_stats()
                return jsonify({
                    'messagesSent': stats.messages_sent,
                    'messagesReceived': stats.messages_received,
                    'messageSendErrors': stats.message_send_errors
                })
            except Exception as e:
                logger.error(f"Error getting counters stats: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/period-stats', methods=['GET'])
        def get_period_stats():
            """Get period stats endpoint."""
            try:
                stats = self.worker.get_period_stats()
                return jsonify(stats.to_dict())
            except Exception as e:
                logger.error(f"Error getting period stats: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/cumulative-latencies', methods=['GET'])
        def get_cumulative_latencies():
            """Get cumulative latencies endpoint."""
            try:
                latencies = self.worker.get_cumulative_latencies()
                return jsonify(latencies.to_dict())
            except Exception as e:
                logger.error(f"Error getting cumulative latencies: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/reset-stats', methods=['POST'])
        def reset_stats():
            """Reset stats endpoint."""
            try:
                self.worker.reset_stats()
                return jsonify({'status': 'ok'})
            except Exception as e:
                logger.error(f"Error resetting stats: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/start-producing', methods=['POST'])
        def start_producing():
            """Signal producers to start producing."""
            try:
                if hasattr(self.worker, 'start_producing_event'):
                    self.worker.start_producing_event.set()
                    return jsonify({'status': 'ok'})
                else:
                    return jsonify({'error': 'Worker does not support start_producing_event'}), 400
            except Exception as e:
                logger.error(f"Error starting production: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/stop-producing', methods=['POST'])
        def stop_producing():
            """Signal producers to stop producing."""
            try:
                if hasattr(self.worker, 'start_producing_event'):
                    self.worker.start_producing_event.clear()
                    return jsonify({'status': 'ok'})
                else:
                    return jsonify({'error': 'Worker does not support start_producing_event'}), 400
            except Exception as e:
                logger.error(f"Error stopping production: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/stop-all', methods=['POST'])
        def stop_all():
            """Stop all endpoint."""
            try:
                self.worker.stop_all()
                return jsonify({'status': 'ok'})
            except Exception as e:
                logger.error(f"Error stopping: {e}")
                return jsonify({'error': str(e)}), 500
