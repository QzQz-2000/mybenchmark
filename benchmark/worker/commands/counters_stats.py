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


class CountersStats:

    def __init__(self):
        self.messages_sent = 0
        self.messages_received = 0
        self.message_send_errors = 0

    def plus(self, to_add: 'CountersStats') -> 'CountersStats':
        result = CountersStats()
        result.messages_sent += self.messages_sent
        result.messages_received += self.messages_received
        result.message_send_errors += self.message_send_errors

        result.messages_sent += to_add.messages_sent
        result.messages_received += to_add.messages_received
        result.message_send_errors += to_add.message_send_errors
        return result
