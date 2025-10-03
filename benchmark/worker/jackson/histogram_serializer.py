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

import base64
import threading
from typing import Any
from hdrh.histogram import HdrHistogram


class HistogramSerializer:
    """
    Serializer for HdrHistogram to JSON.
    Equivalent to Jackson's HistogramSerializer in Java.
    """

    def __init__(self):
        # Thread-local buffer (8 MB initial size)
        self._thread_local = threading.local()

    def _get_buffer(self) -> bytearray:
        """Get thread-local buffer, create if doesn't exist."""
        if not hasattr(self._thread_local, 'buffer'):
            self._thread_local.buffer = bytearray(8 * 1024 * 1024)
        return self._thread_local.buffer

    @staticmethod
    def serialize_histogram(histo: HdrHistogram) -> bytes:
        """
        Serialize histogram to compressed byte array.

        :param histo: The histogram to serialize
        :return: Compressed byte array
        """
        # Use HdrHistogram's built-in encoding to compressed format
        # This returns a base64-encoded compressed representation
        encoded = histo.encode()
        return base64.b64decode(encoded)

    def to_json(self, histogram: HdrHistogram) -> str:
        """
        Convert histogram to JSON-compatible base64 string.

        :param histogram: The histogram to serialize
        :return: Base64-encoded string
        """
        compressed_bytes = self.serialize_histogram(histogram)
        return base64.b64encode(compressed_bytes).decode('ascii')

    def __call__(self, histogram: HdrHistogram) -> str:
        """Allow serializer to be called as a function."""
        return self.to_json(histogram)
