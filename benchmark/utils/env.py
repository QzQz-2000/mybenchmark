import os
from typing import TypeVar, Callable, Optional

T = TypeVar('T')


class Env:

    def __init__(self):
        # Private constructor equivalent - prevent instantiation
        raise RuntimeError("Env class should not be instantiated")

    @staticmethod
    def get_long(key: str, default_value: int) -> int:
        return Env.get(key, int, default_value)

    @staticmethod
    def get_double(key: str, default_value: float) -> float:
        return Env.get(key, float, default_value)

    @staticmethod
    def get(key: str, function: Callable[[str], T], default_value: T) -> T:
        env_value = os.getenv(key)
        if env_value is not None:
            try:
                return function(env_value)
            except (ValueError, TypeError):
                return default_value
        return default_value
