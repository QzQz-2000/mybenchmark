import json
from typing import Any, Type
from hdrh.histogram import HdrHistogram
from .histogram_serializer import HistogramSerializer
from .histogram_deserializer import HistogramDeserializer


class ObjectMappers:
    """
    Singleton object mapper with custom Histogram serialization.
    Equivalent to Java's ObjectMappers enum singleton pattern.
    """

    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not ObjectMappers._initialized:
            # Initialize serializers/deserializers
            self._histogram_serializer = HistogramSerializer()
            self._histogram_deserializer = HistogramDeserializer()
            ObjectMappers._initialized = True

    def mapper(self) -> 'ObjectMappers':
        """
        Get the mapper instance (for API compatibility).

        :return: self
        """
        return self

    def writer(self) -> 'ObjectMappers':
        """
        Get the writer instance with pretty printing.

        :return: self
        """
        return self

    def dumps(self, obj: Any, **kwargs) -> str:
        """
        Serialize object to JSON string.

        :param obj: Object to serialize
        :param kwargs: Additional arguments for json.dumps
        :return: JSON string
        """
        # Set default pretty printing
        if 'indent' not in kwargs:
            kwargs['indent'] = 2

        # Custom encoder for HdrHistogram
        def default_encoder(o):
            if isinstance(o, HdrHistogram):
                return self._histogram_serializer(o)
            # Handle other custom types if needed
            raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")

        kwargs['default'] = default_encoder
        return json.dumps(obj, **kwargs)

    def loads(self, json_str: str, target_class: Type = None, **kwargs) -> Any:
        """
        Deserialize JSON string to object.

        :param json_str: JSON string
        :param target_class: Target class for deserialization (optional)
        :param kwargs: Additional arguments for json.loads
        :return: Deserialized object
        """
        # Custom decoder hook for HdrHistogram
        def object_hook(obj_dict):
            # If we detect histogram data (base64 string), deserialize it
            # This is a simplified approach - you may need to enhance based on your schema
            for key, value in obj_dict.items():
                if isinstance(value, str) and key.endswith('_latency'):
                    try:
                        obj_dict[key] = self._histogram_deserializer(value)
                    except Exception:
                        # Not a histogram, keep as-is
                        pass
            return obj_dict

        kwargs['object_hook'] = object_hook
        return json.loads(json_str, **kwargs)

    def write_value_as_string(self, obj: Any) -> str:
        """
        Jackson-compatible method name for serialization.

        :param obj: Object to serialize
        :return: JSON string
        """
        return self.dumps(obj)

    def read_value(self, json_str: str, target_class: Type = None) -> Any:
        """
        Jackson-compatible method name for deserialization.

        :param json_str: JSON string
        :param target_class: Target class (optional)
        :return: Deserialized object
        """
        return self.loads(json_str, target_class)


# Singleton instance (similar to Java's enum pattern)
DEFAULT = ObjectMappers()
