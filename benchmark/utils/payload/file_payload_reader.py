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

from pathlib import Path
from .payload_reader import PayloadReader
from .payload_exception import PayloadException


class FilePayloadReader(PayloadReader):

    def __init__(self, expected_length: int):
        self.expected_length = expected_length

    def load(self, resource_name: str) -> bytes:
        try:
            payload = Path(resource_name).read_bytes()
            self._check_payload_length(payload)
            return payload
        except IOError as e:
            raise PayloadException(str(e))

    def _check_payload_length(self, payload: bytes):
        if self.expected_length != len(payload):
            raise PayloadException(
                f"Payload length mismatch. Actual is: {len(payload)}, but expected: {self.expected_length} "
            )
