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
                logger.error(f"Error initializing driver: {e}")
                return jsonify({'error': str(e)}), 500

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

            data = request.json
            assignment = ProducerWorkAssignment()
            assignment.publish_rate = data['publishRate']
            # TODO: Set keyDistributorType and payloadData

            try:
                self.worker.start_load(assignment)
                return jsonify({'status': 'ok'})
            except Exception as e:
                logger.error(f"Error starting load: {e}")
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

        @self.app.route('/reset-stats', methods=['POST'])
        def reset_stats():
            """Reset stats endpoint."""
            try:
                self.worker.reset_stats()
                return jsonify({'status': 'ok'})
            except Exception as e:
                logger.error(f"Error resetting stats: {e}")
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
