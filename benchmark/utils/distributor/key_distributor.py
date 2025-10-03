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

from abc import ABC, abstractmethod
import base64
import random
from typing import Optional
from .key_distributor_type import KeyDistributorType


class KeyDistributor(ABC):

    UNIQUE_COUNT = 10_000
    KEY_BYTE_SIZE = 7

    _random_keys = []

    @classmethod
    def _initialize_random_keys(cls):
        """Generate a number of random keys to be used when publishing"""
        if not cls._random_keys:
            random_gen = random.Random()
            for _ in range(cls.UNIQUE_COUNT):
                buffer = random_gen.randbytes(cls.KEY_BYTE_SIZE)
                # Equivalent to BaseEncoding.base64Url().omitPadding().encode(buffer)
                encoded = base64.urlsafe_b64encode(buffer).decode('ascii').rstrip('=')
                cls._random_keys.append(encoded)

    def __init__(self):
        KeyDistributor._initialize_random_keys()

    def get(self, index: int) -> str:
        return self._random_keys[index]

    def get_length(self) -> int:
        return self.UNIQUE_COUNT

    @abstractmethod
    def next(self) -> Optional[str]:
        pass

    @staticmethod
    def build(key_type: KeyDistributorType) -> 'KeyDistributor':
        from .no_key_distributor import NoKeyDistributor
        from .key_round_robin import KeyRoundRobin
        from .random_nano import RandomNano

        if key_type == KeyDistributorType.NO_KEY:
            return NoKeyDistributor()
        elif key_type == KeyDistributorType.KEY_ROUND_ROBIN:
            return KeyRoundRobin()
        elif key_type == KeyDistributorType.RANDOM_NANO:
            return RandomNano()
        else:
            raise ValueError(f"Unexpected KeyDistributorType: {key_type}")
