from setuptools import setup, find_packages

setup(
    name="openmessaging-benchmark",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        'pyyaml>=5.1',
        'kafka-python>=2.0.2',
        'hdrhistogram>=0.10.3',
        'requests>=2.25.0',
        'Flask>=2.0.0',
    ],
    entry_points={
        'console_scripts': [
            'messaging-benchmark=benchmark.benchmark:main',
        ],
    },
    python_requires='>=3.7',
)
