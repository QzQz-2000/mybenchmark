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

# 这个文件是程序的入口点
# 当你在命令行运行 "python -m benchmark" 时，Python会执行这个文件

import logging
import sys

# 从benchmark包中导入Benchmark类（这是主程序类）
from benchmark.benchmark import Benchmark

# __name__ == '__main__' 的意思是：只有直接运行这个文件时才执行下面的代码
# 如果这个文件被别的代码导入，下面的代码不会执行
if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # 调用Benchmark类的main方法，并传入命令行参数
    # sys.argv[1:] 的意思是：从第二个参数开始取（去掉第一个程序名）
    # 例如：sys.argv = ['benchmark', '-d', 'kafka.yaml', 'workload.yaml']
    #      sys.argv[1:] = ['-d', 'kafka.yaml', 'workload.yaml']
    Benchmark.main(sys.argv[1:])
