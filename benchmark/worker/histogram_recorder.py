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

"""
HistogramRecorder - 模仿Java HdrHistogram的Recorder模式
使用双缓冲机制实现高性能的周期统计
"""

import threading
from hdrh.histogram import HdrHistogram


class HistogramRecorder:
    """
    双缓冲Histogram Recorder（模仿Java版本）

    原理：
    1. 维护两个Histogram：active（活跃）和inactive（非活跃）
    2. 记录数据时写入active
    3. 获取周期统计时，swap两个指针（O(1)时间），返回旧的active
    4. 重置inactive为新的空Histogram

    相比直接使用Histogram的优势：
    - Java版本：getIntervalHistogram() 只是swap指针，极快
    - Python旧版本：encode() + decode() 需要序列化整个数据结构，很慢
    - 这个实现：swap指针，和Java一样快
    """

    def __init__(self, lowest_trackable_value: int, highest_trackable_value: int,
                 significant_figures: int):
        """
        初始化Recorder

        :param lowest_trackable_value: 最小可追踪值
        :param highest_trackable_value: 最大可追踪值
        :param significant_figures: 有效数字位数（精度）
        """
        self.lowest = lowest_trackable_value
        self.highest = highest_trackable_value
        self.precision = significant_figures

        # 双缓冲：active用于记录，inactive用于返回
        self.active = HdrHistogram(lowest_trackable_value, highest_trackable_value, significant_figures)
        self.inactive = HdrHistogram(lowest_trackable_value, highest_trackable_value, significant_figures)

        self.lock = threading.Lock()

    def record_value(self, value: int):
        """
        记录一个值到active histogram

        :param value: 要记录的值（微秒）
        """
        with self.lock:
            self.active.record_value(value)

    def get_interval_histogram(self) -> HdrHistogram:
        """
        获取周期Histogram（模仿Java Recorder.getIntervalHistogram()）

        实现：
        1. 交换active和inactive的引用（O(1)）
        2. 返回旧的active（现在变成inactive了）
        3. 创建新的inactive histogram，准备下次使用

        :return: 包含上个周期所有样本的Histogram
        """
        with self.lock:
            # 保存旧的active（等下要返回）
            old_active = self.active

            # 交换：inactive变成新的active，active变成要返回的
            self.active = self.inactive

            # 创建新的inactive（下次swap时用）
            self.inactive = HdrHistogram(self.lowest, self.highest, self.precision)

            # 返回旧的active（包含上个周期的所有数据）
            return old_active

    def get_interval_histogram_into(self, target: HdrHistogram) -> HdrHistogram:
        """
        获取周期Histogram并合并到目标histogram中
        这个方法同时返回周期histogram，并将数据累积到target中

        :param target: 累积目标histogram（通常是cumulative histogram）
        :return: 周期histogram
        """
        interval = self.get_interval_histogram()

        # 将周期数据添加到累积histogram中
        with self.lock:
            target.add(interval)

        return interval

    def record_histogram(self, histogram: HdrHistogram):
        """
        将另一个histogram的所有数据记录到active histogram中
        用于合并Agent进程发送过来的histogram数据

        :param histogram: 要合并的histogram
        """
        with self.lock:
            self.active.add(histogram)

    def reset(self):
        """重置recorder（清空所有数据）"""
        with self.lock:
            self.active = HdrHistogram(self.lowest, self.highest, self.precision)
            self.inactive = HdrHistogram(self.lowest, self.highest, self.precision)
