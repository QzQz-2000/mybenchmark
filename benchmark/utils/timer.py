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

import time
from typing import Callable, Optional


class Timer:

    def __init__(self, nano_clock: Optional[Callable[[], int]] = None):
        """
        Create a Timer.

        :param nano_clock: Optional nanosecond clock function. Defaults to time.perf_counter_ns
        """
        self.nano_clock = nano_clock if nano_clock is not None else time.perf_counter_ns
        self.start_time = self.nano_clock()

    def elapsed_millis(self) -> float:
        """
        Get elapsed time in milliseconds.

        :return: Elapsed time in milliseconds
        """
        return self._elapsed(1_000_000)  # 1 millisecond = 1,000,000 nanoseconds

    def elapsed_seconds(self) -> float:
        """
        Get elapsed time in seconds.

        :return: Elapsed time in seconds
        """
        return self._elapsed(1_000_000_000)  # 1 second = 1,000,000,000 nanoseconds

    def _elapsed(self, nanos_per_unit: int) -> float:
        """
        Calculate elapsed time in the given unit.

        :param nanos_per_unit: Nanoseconds per time unit
        :return: Elapsed time
        """
        now = self.nano_clock()
        return (now - self.start_time) / float(nanos_per_unit)
