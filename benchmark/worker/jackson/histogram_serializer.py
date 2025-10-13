import base64
from typing import Any
from hdrh.histogram import HdrHistogram


class HistogramSerializer:
    """
    Serializer for HdrHistogram to JSON.
    Equivalent to Jackson's HistogramSerializer in Java.
    """

    def __init__(self):
        # Process-local buffer (8 MB initial size)
        # Each process gets its own independent buffer
        self._buffer = None

    def _get_buffer(self) -> bytearray:
        """Get process-local buffer, create if doesn't exist."""
        if self._buffer is None:
            self._buffer = bytearray(8 * 1024 * 1024)
        return self._buffer

    @staticmethod
    def serialize_histogram(histo: HdrHistogram) -> bytes:
        """
        Serialize histogram to compressed byte array (raw binary).

        :param histo: The histogram to serialize
        :return: Compressed byte array (raw binary format)
        """
        # Python's histo.encode() returns base64 string
        # Decode to get raw compressed bytes (equivalent to Java's encodeIntoCompressedByteBuffer)
        encoded = histo.encode()
        return base64.b64decode(encoded)

    def to_json(self, histogram: HdrHistogram) -> str:
        """
        Convert histogram to JSON-compatible base64 string.

        :param histogram: The histogram to serialize
        :return: Base64-encoded string
        """
        # Direct encoding without unnecessary decode/encode cycle
        # histo.encode() already returns base64 string, use it directly!
        return histogram.encode()

    def __call__(self, histogram: HdrHistogram) -> str:
        """Allow serializer to be called as a function."""
        return self.to_json(histogram)
