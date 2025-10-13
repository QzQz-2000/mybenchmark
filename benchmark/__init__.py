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

# 这个文件是benchmark包的初始化文件
# 作用：定义这个包对外暴露哪些类，方便其他代码导入使用

# 从benchmark.py文件中导入Benchmark类（主程序类，负责运行整个性能测试）
from .benchmark import Benchmark

# 从driver_configuration.py导入DriverConfiguration类（驱动配置类，存储Kafka等消息系统的配置信息）
from .driver_configuration import DriverConfiguration

# 从rate_controller.py导入RateController类（速率控制器，控制每秒发送多少条消息）
from .rate_controller import RateController

# 从results_to_csv.py导入ResultsToCsv类（结果转换类，把测试结果保存为CSV文件）
from .results_to_csv import ResultsToCsv

# 从test_result.py导入TestResult类（测试结果类，存储测试的统计数据如延迟、吞吐量等）
from .test_result import TestResult

# 从workload.py导入Workload类（工作负载类，定义测试参数如消息大小、发送速率等）
from .workload import Workload

# 从workload_generator.py导入WorkloadGenerator类（负载生成器，执行实际的消息发送和接收）
from .workload_generator import WorkloadGenerator

# 从workers.py导入Workers类（Worker配置类，管理分布式测试的worker节点）
from .workers import Workers

# __all__是一个特殊变量，定义了当别人使用"from benchmark import *"时能导入哪些类
# 这是一个列表，包含了上面导入的所有类名
__all__ = [
    'Benchmark',           # 主程序类
    'DriverConfiguration', # 驱动配置类
    'RateController',      # 速率控制类
    'ResultsToCsv',        # 结果转CSV类
    'TestResult',          # 测试结果类
    'Workload',            # 工作负载类
    'WorkloadGenerator',   # 负载生成器类
    'Workers'              # Worker配置类
]
