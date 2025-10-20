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

import logging
import os

logger = logging.getLogger(__name__)


class RateController:

    ONE_SECOND_IN_NANOS = 1_000_000_000

    def __init__(self):
        self.publish_backlog_limit = int(os.getenv('PUBLISH_BACKLOG_LIMIT', '1000'))
        self.receive_backlog_limit = int(os.getenv('RECEIVE_BACKLOG_LIMIT', '1000'))
        self.min_ramping_factor = float(os.getenv('MIN_RAMPING_FACTOR', '0.01'))
        self.max_ramping_factor = float(os.getenv('MAX_RAMPING_FACTOR', '1'))
        self.ramping_factor = self.max_ramping_factor

        # 最小速率保护：防止速率降为 0
        self.min_rate = float(os.getenv('MIN_PUBLISH_RATE', '100'))

        self.previous_total_published = 0
        self.previous_total_received = 0

    def get_ramping_factor(self) -> float:
        return self.ramping_factor

    def next_rate(self, rate: float, period_nanos: int, total_published: int, total_received: int) -> float:
        expected = int((rate / self.ONE_SECOND_IN_NANOS) * period_nanos)
        published = total_published - self.previous_total_published
        received = total_received - self.previous_total_received

        self.previous_total_published = total_published
        self.previous_total_received = total_received

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Current rate: {rate} -- Publish rate {self._rate(published, period_nanos)} -- "
                f"Receive Rate: {self._rate(received, period_nanos)}"
            )

        receive_backlog = total_published - total_received
        if receive_backlog > self.receive_backlog_limit:
            return self._next_rate(period_nanos, received, expected, receive_backlog, "Receive")

        publish_backlog = expected - published
        if publish_backlog > self.publish_backlog_limit:
            return self._next_rate(period_nanos, published, expected, publish_backlog, "Publish")

        self._ramp_up()

        return rate + (rate * self.ramping_factor)

    def _next_rate(self, period_nanos: int, actual: int, expected: int, backlog: int, type_name: str) -> float:
        logger.debug(f"{type_name} backlog: {backlog}")
        self._ramp_down()
        next_expected = max(0, expected - backlog)
        next_expected_rate = self._rate(next_expected, period_nanos)
        actual_rate = self._rate(actual, period_nanos)
        calculated_rate = min(actual_rate, next_expected_rate)

        # 确保速率不会降到最小值以下
        return max(self.min_rate, calculated_rate)

    def _rate(self, count: int, period_nanos: int) -> float:
        return (count / float(period_nanos)) * self.ONE_SECOND_IN_NANOS

    def _ramp_up(self):
        self.ramping_factor = min(self.max_ramping_factor, self.ramping_factor * 2)

    def _ramp_down(self):
        self.ramping_factor = max(self.min_ramping_factor, self.ramping_factor / 2)
