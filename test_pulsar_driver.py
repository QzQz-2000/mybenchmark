#!/usr/bin/env python3
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
Test script for Pulsar driver

This script tests basic functionality of the Pulsar driver without running a full benchmark.
"""

import logging
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def test_pulsar_driver():
    """Test Pulsar driver initialization and basic operations."""
    try:
        from benchmark.drivers.pulsar import PulsarBenchmarkDriver

        logger.info("=" * 80)
        logger.info("Testing Pulsar Benchmark Driver")
        logger.info("=" * 80)

        # Initialize driver
        logger.info("\n1. Initializing driver...")
        driver = PulsarBenchmarkDriver()
        driver.initialize('examples/pulsar-driver.yaml')

        logger.info(f"✓ Driver initialized successfully")
        logger.info(f"  Namespace: {driver.namespace}")
        logger.info(f"  Topic prefix: {driver.get_topic_name_prefix()}")

        # Test creating a topic
        logger.info("\n2. Creating test topic...")
        topic_name = driver.get_topic_name_prefix() + "-0"
        create_future = driver.create_topics([{'topic': topic_name, 'partitions': 1}])
        topics = create_future.result()
        logger.info(f"✓ Topics created: {topics}")

        # Test creating a producer
        logger.info("\n3. Creating producer...")
        producer_future = driver.create_producer(topic_name)
        producer = producer_future.result()
        logger.info(f"✓ Producer created for topic: {topic_name}")

        # Test sending a message
        logger.info("\n4. Sending test message...")
        test_message = b"Hello Pulsar from Python Benchmark!"
        send_future = producer.send_async(None, test_message)
        send_future.result()
        logger.info(f"✓ Message sent successfully")

        # Close producer
        logger.info("\n5. Closing producer...")
        producer.close()
        logger.info(f"✓ Producer closed")

        # Close driver
        logger.info("\n6. Closing driver...")
        driver.close()
        logger.info(f"✓ Driver closed")

        logger.info("\n" + "=" * 80)
        logger.info("✓ All tests passed!")
        logger.info("=" * 80)

        return True

    except ImportError as e:
        logger.error(f"\n❌ Import error: {e}")
        logger.error("   Please install pulsar-client: pip install pulsar-client")
        return False

    except Exception as e:
        logger.error(f"\n❌ Test failed: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = test_pulsar_driver()
    sys.exit(0 if success else 1)
