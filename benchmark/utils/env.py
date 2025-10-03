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
