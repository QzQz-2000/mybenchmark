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

import json
import logging
import os
import time
from pathlib import Path
from typing import List
from hdrh.histogram import HdrHistogram
from .test_result import TestResult

logger = logging.getLogger(__name__)


class ResultsToCsv:

    def write_all_result_files(self, directory: str):
        try:
            dir_path = Path(directory)
            if not dir_path.is_dir():
                raise ValueError(f"Not a directory: {directory}")

            directory_listing = sorted(dir_path.iterdir())

            lines = []
            lines.append(
                "topics,partitions,message-size,producers-per-topic,consumers-per-topic,"
                + "prod-rate-min,prod-rate-avg,prod-rate-std-dev,prod-rate-max,"
                + "con-rate-min,con-rate-avg,con-rate-std-dev,con-rate-max,"
            )

            results: List[TestResult] = []
            for file_path in directory_listing:
                if file_path.is_file() and str(file_path).endswith(".json"):
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        tr = self._json_to_test_result(data)
                        results.append(tr)

            sorted_results = sorted(
                results,
                key=lambda x: (x.get_message_size(), x.get_topics(), x.get_partitions())
            )

            for tr in sorted_results:
                lines.append(self.extract_results(tr))

            results_file_name = f"results-{int(time.time())}.csv"
            with open(results_file_name, 'w') as writer:
                for line in lines:
                    writer.write(line + os.linesep)

            logger.info(f"Results extracted into CSV {results_file_name}")

        except IOError as e:
            logger.error(f"Failed creating csv file: {e}")

    def extract_results(self, tr: TestResult) -> str:
        try:
            prod_rate_histogram = HdrHistogram(1, 10000000, 1)
            con_rate_histogram = HdrHistogram(1, 10000000, 1)

            for rate in tr.publish_rate:
                prod_rate_histogram.record_value(int(rate))
                prod_rate_histogram.record_value(int(rate))  # Count as 2

            for rate in tr.consume_rate:
                con_rate_histogram.record_value(int(rate))
                con_rate_histogram.record_value(int(rate))  # Count as 2

            line = (
                f"{tr.topics},"
                f"{tr.partitions},"
                f"{tr.message_size},"
                f"{tr.producers_per_topic},"
                f"{tr.consumers_per_topic},"
                f"{prod_rate_histogram.get_min_value()},"
                f"{prod_rate_histogram.get_mean_value():.0f},"
                f"{prod_rate_histogram.get_stddev():.2f},"
                f"{prod_rate_histogram.get_max_value()},"
                f"{con_rate_histogram.get_min_value()},"
                f"{con_rate_histogram.get_mean_value():.0f},"
                f"{con_rate_histogram.get_stddev():.2f},"
                f"{con_rate_histogram.get_max_value()}"
            )

            return line
        except Exception as e:
            logger.error(f"Error writing results csv: {e}")
            raise RuntimeError(e) from e

    def _json_to_test_result(self, data: dict) -> TestResult:
        """Convert JSON dict to TestResult object."""
        tr = TestResult()
        tr.workload = data.get('workload')
        tr.driver = data.get('driver')
        tr.message_size = data.get('messageSize', 0)
        tr.topics = data.get('topics', 0)
        tr.partitions = data.get('partitions', 0)
        tr.producers_per_topic = data.get('producersPerTopic', 0)
        tr.consumers_per_topic = data.get('consumersPerTopic', 0)

        tr.publish_rate = data.get('publishRate', [])
        tr.publish_error_rate = data.get('publishErrorRate', [])
        tr.consume_rate = data.get('consumeRate', [])
        tr.backlog = data.get('backlog', [])

        # Add other fields as needed
        return tr
