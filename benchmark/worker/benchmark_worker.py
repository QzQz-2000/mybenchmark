import argparse
import logging
import json
from flask import Flask
from .worker_handler import WorkerHandler

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
        self.worker_handler = None

    def start(self):
        """Start the benchmark worker HTTP server."""
        logger.info(f"Starting benchmark worker on port {self.http_port}")

        # Create Flask app
        self.app = Flask(__name__)

        # Initialize worker handler
        # TODO: Implement Prometheus metrics provider for stats_logger
        self.worker_handler = WorkerHandler(self.app, stats_logger=None)

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
