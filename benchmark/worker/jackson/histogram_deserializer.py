import base64
import threading
import logging
from typing import Union
from hdrh.histogram import HdrHistogram

logger = logging.getLogger(__name__)


class HistogramDeserializer:
    """
    Deserializer for HdrHistogram from JSON.
    Equivalent to Jackson's HistogramDeserializer in Java.
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
    def deserialize_histogram(data: bytes) -> HdrHistogram:
        """
        Deserialize histogram from compressed byte array (raw binary).

        :param data: Compressed byte array (raw binary format)
        :return: Deserialized histogram
        """
        try:
            # Python's HdrHistogram.decode() expects base64-encoded string
            # Java's decodeFromCompressedByteBuffer() uses raw binary
            # So we need to convert raw binary to base64 string
            encoded_str = base64.b64encode(data).decode('ascii')
            return HdrHistogram.decode(encoded_str)
        except Exception as e:
            logger.error(f"Failed to decode histogram: {e}")
            # Log hex dump for debugging
            hex_dump = data.hex()
            logger.error(f"Hex dump: {hex_dump[:200]}...")
            raise RuntimeError(f"Failed to deserialize histogram: {e}") from e

    def from_json(self, json_value: Union[str, bytes]) -> HdrHistogram:
        """
        Convert JSON-compatible value to histogram.

        :param json_value: Base64-encoded string or raw compressed bytes
        :return: Deserialized histogram
        """
        if isinstance(json_value, str):
            # If already base64 string, decode directly (no double encoding!)
            return HdrHistogram.decode(json_value)
        else:
            # If raw binary bytes, convert to base64 first
            return self.deserialize_histogram(json_value)

    def __call__(self, json_value: Union[str, bytes]) -> HdrHistogram:
        """Allow deserializer to be called as a function."""
        return self.from_json(json_value)
