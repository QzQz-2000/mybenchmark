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
A benchmark worker that listens for tasks to perform.
"""

import argparse
import logging
import json
from flask import Flask
# from .worker_handler import WorkerHandler

logger = logging.getLogger(__name__)


class BenchmarkWorker:
    """Main benchmark worker application."""

    def __init__(self, http_port: int = 8080, stats_port: int = 8081):
        """
        Initialize benchmark worker.

        :param http_port: HTTP port for worker API
        :param stats_port: Stats/metrics port
        """
        self.http_port = http_port
        self.stats_port = stats_port
        self.app = None

    def start(self):
        """Start the benchmark worker HTTP server."""
        logger.info(f"Starting benchmark worker on port {self.http_port}")

        # Create Flask app
        self.app = Flask(__name__)

        # Initialize worker handler
        # TODO: Implement Prometheus metrics provider
        # worker_handler = WorkerHandler(self.app, stats_logger)

        # Start Flask server
        self.app.run(host='0.0.0.0', port=self.http_port, threaded=True)


def main():
    """Main entry point for benchmark worker."""
    parser = argparse.ArgumentParser(description='Benchmark Worker')
    parser.add_argument('-p', '--port', type=int, default=8080,
                       help='HTTP port to listen on')
    parser.add_argument('--stats-port', type=int, default=8081,
                       help='Stats port to listen on')

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Log configuration
    logger.info(f"Starting benchmark with config: {json.dumps(vars(args), indent=2)}")

    # Start worker
    worker = BenchmarkWorker(http_port=args.port, stats_port=args.stats_port)
    worker.start()


if __name__ == '__main__':
    main()
