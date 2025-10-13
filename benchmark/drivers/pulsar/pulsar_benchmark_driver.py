# Licensed under the Apache License, Version 2.0 (the "License")
# You may not use this file except in compliance with the License.
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
Pulsar Benchmark Driver - Python implementation
"""

import logging
import yaml
import pulsar
import base64
import os
from concurrent.futures import Future
from typing import List, Dict, Any
import requests

logger = logging.getLogger(__name__)


class PulsarBenchmarkDriver:
    """
    Pulsar Benchmark Driver

    Implements the benchmark driver interface for Apache Pulsar.
    """

    def __init__(self):
        self.client = None
        self.config = None
        self.namespace = None
        self.producer_builder_config = None
        self.http_url = None  # Store HTTP URL for admin operations
        self.use_fixed_namespace = False  # Whether to use fixed namespace for idempotency

    def initialize(self, configuration_file: str, stats_logger=None):
        """
        Initialize the Pulsar driver.

        :param configuration_file: Path to YAML configuration file
        :param stats_logger: Optional stats logger
        """
        # Load configuration
        with open(configuration_file, 'r') as f:
            self.config = yaml.safe_load(f)

        logger.info(f"Pulsar driver configuration: {self.config}")

        # Extract client configuration
        client_config = self.config.get('client', {})
        producer_config = self.config.get('producer', {})

        # Create Pulsar client
        service_url = client_config.get('serviceUrl', 'pulsar://localhost:6650')
        client_kwargs = {
            'service_url': service_url,
            'io_threads': client_config.get('ioThreads', 16),
            'operation_timeout_seconds': 600,
        }

        # TLS
        if service_url.startswith('pulsar+ssl'):
            tls_trust_certs = client_config.get('tlsTrustCertsFilePath')
            if tls_trust_certs:
                client_kwargs['tls_trust_certs_file_path'] = tls_trust_certs
            client_kwargs['tls_allow_insecure_connection'] = client_config.get('tlsAllowInsecureConnection', False)
            client_kwargs['tls_validate_hostname'] = client_config.get('tlsEnableHostnameVerification', False)

        # Authentication
        auth_config = client_config.get('authentication', {})
        if auth_config.get('plugin'):
            client_kwargs['authentication'] = pulsar.AuthenticationToken(auth_config.get('data', ''))

        self.client = pulsar.Client(**client_kwargs)
        logger.info(f"Created Pulsar client for service URL {service_url}")

        # Store producer configuration
        self.producer_builder_config = {
            'batching_enabled': producer_config.get('batchingEnabled', True),
            'batching_max_publish_delay_ms': producer_config.get('batchingMaxPublishDelayMs', 1),
            'batching_max_bytes': producer_config.get('batchingMaxBytes', 131072),
            'block_if_queue_full': producer_config.get('blockIfQueueFull', True),
            'max_pending_messages': producer_config.get('pendingQueueSize', 1000),
            'send_timeout_millis': 0,
        }

        # Store HTTP URL for admin operations
        self.http_url = client_config.get('httpUrl', 'http://localhost:8080')

        # Create namespace via HTTP Admin API
        namespace_prefix = client_config.get('namespacePrefix', 'benchmark/ns')
        self.use_fixed_namespace = client_config.get('useFixedNamespace', False)

        if self.use_fixed_namespace:
            # Use fixed namespace for idempotency (enables proper cleanup)
            self.namespace = namespace_prefix
            logger.info(f"Using FIXED Pulsar namespace: {self.namespace} (idempotent mode)")
        else:
            # Use random namespace (legacy behavior, topics will accumulate)
            random_suffix = base64.urlsafe_b64encode(os.urandom(5)).decode('utf-8').rstrip('=')
            self.namespace = f"{namespace_prefix}-{random_suffix}"
            logger.info(f"Using RANDOM Pulsar namespace: {self.namespace} (topics will accumulate)")

        self._create_namespace_http(self.namespace, self.http_url)

    def _create_namespace_http(self, namespace: str, http_url: str):
        """
        Create Pulsar namespace via HTTP admin API.
        Also creates tenant if it doesn't exist.
        """
        # Extract tenant from namespace (format: tenant/namespace)
        parts = namespace.split('/')
        if len(parts) < 2:
            raise RuntimeError(f"Invalid namespace format: {namespace}. Expected format: tenant/namespace")

        tenant = parts[0]

        # Step 1: Create tenant if it doesn't exist
        tenant_url = f"{http_url}/admin/v2/tenants/{tenant}"
        try:
            tenant_resp = requests.get(tenant_url)
            if tenant_resp.status_code == 404:
                # Tenant doesn't exist, create it
                logger.info(f"Tenant {tenant} not found, creating...")
                create_tenant_resp = requests.put(
                    tenant_url,
                    json={"allowedClusters": ["standalone"]},  # Default for standalone
                    headers={"Content-Type": "application/json"}
                )
                if create_tenant_resp.status_code in (204, 409):
                    logger.info(f"✅ Tenant {tenant} created or already exists")
                else:
                    logger.warning(f"⚠️  Failed to create tenant {tenant}: {create_tenant_resp.status_code} {create_tenant_resp.text}")
            elif tenant_resp.status_code == 200:
                logger.info(f"✅ Tenant {tenant} already exists")
        except Exception as e:
            logger.warning(f"⚠️  Could not verify/create tenant {tenant}: {e}")

        # Step 2: Create namespace
        url = f"{http_url}/admin/v2/namespaces/{namespace}"
        try:
            resp = requests.put(url)
            if resp.status_code in (204, 409):  # 204=created, 409=already exists
                logger.info(f"✅ Namespace {namespace} is ready (created or already exists)")
            else:
                raise RuntimeError(f"Failed to create namespace {namespace}: {resp.status_code} {resp.text}")
        except Exception as e:
            raise RuntimeError(f"Error creating namespace {namespace} via HTTP: {e}")

    def get_topic_name_prefix(self) -> str:
        """
        Get the topic name prefix.

        :return: Topic name prefix
        """
        topic_type = self.config.get('client', {}).get('topicType', 'persistent')
        return f"{topic_type}://{self.namespace}/test"

    def get_producer_properties(self) -> Dict[str, Any]:
        """
        Get producer properties for isolated agents.

        :return: Dictionary of producer configuration
        """
        return self.producer_builder_config.copy()

    def get_consumer_properties(self) -> Dict[str, Any]:
        """
        Get consumer properties for isolated agents.

        :return: Dictionary of consumer configuration
        """
        consumer_config = self.config.get('consumer', {})
        return {
            'receiverQueueSize': consumer_config.get('receiverQueueSize', 1000),
            'subscriptionType': consumer_config.get('subscriptionType', 'Failover'),
        }

    def get_client_properties(self) -> Dict[str, Any]:
        """
        Get client properties for isolated agents.

        :return: Dictionary of client configuration
        """
        client_config = self.config.get('client', {})
        service_url = client_config.get('serviceUrl', 'pulsar://localhost:6650')

        result = {
            'serviceUrl': service_url,
            'ioThreads': client_config.get('ioThreads', 1),
            'operationTimeoutSeconds': 600,
        }

        # TLS configuration
        if service_url.startswith('pulsar+ssl'):
            tls_trust_certs = client_config.get('tlsTrustCertsFilePath')
            if tls_trust_certs:
                result['tlsTrustCertsFilePath'] = tls_trust_certs
            result['tlsAllowInsecureConnection'] = client_config.get('tlsAllowInsecureConnection', False)
            result['tlsEnableHostnameVerification'] = client_config.get('tlsEnableHostnameVerification', False)

        # Authentication configuration
        auth_config = client_config.get('authentication', {})
        if auth_config.get('plugin'):
            result['authenticationToken'] = auth_config.get('data', '')

        return result

    def create_topics(self, topic_infos: List[Dict[str, Any]]) -> Future:
        """
        Create topics.

        :param topic_infos: List of topic information dictionaries
        :return: Future that completes when topics are created
        """
        future = Future()
        try:
            topics = [info['topic'] for info in topic_infos]
            logger.info(f"Topics to be created (will auto-create): {topics}")
            future.set_result(topics)
        except Exception as e:
            logger.error(f"Error creating topics: {e}")
            future.set_exception(e)
        return future

    def delete_topics(self, topics: List[str]) -> Future:
        """
        Delete topics via Pulsar Admin API.

        :param topics: List of topic names to delete
        :return: Future that completes when topics are deleted
        """
        future = Future()
        try:
            if not topics:
                logger.info("No topics to delete")
                future.set_result(None)
                return future

            logger.info(f"Deleting {len(topics)} topics via Pulsar Admin API: {topics}")

            deleted_count = 0
            failed_count = 0
            not_found_count = 0

            for topic in topics:
                try:
                    # Convert topic format: persistent://tenant/namespace/topic -> persistent/tenant/namespace/topic
                    topic_path = topic.replace('://', '/')

                    # Delete topic (non-partitioned) or all partitions
                    url = f"{self.http_url}/admin/v2/{topic_path}"

                    # Try to delete with force flag to remove all subscriptions
                    resp = requests.delete(url, params={'force': 'true', 'deleteSchema': 'true'})

                    if resp.status_code == 204:
                        deleted_count += 1
                        logger.debug(f"✅ Deleted topic: {topic}")
                    elif resp.status_code == 404:
                        not_found_count += 1
                        logger.debug(f"⚠️  Topic not found (already deleted or never existed): {topic}")
                    else:
                        failed_count += 1
                        logger.warning(f"❌ Failed to delete topic {topic}: {resp.status_code} {resp.text}")

                except Exception as e:
                    failed_count += 1
                    logger.warning(f"❌ Error deleting topic {topic}: {e}")

            logger.info(
                f"Topic deletion summary: {deleted_count} deleted, "
                f"{not_found_count} not found, {failed_count} failed"
            )

            future.set_result(None)

        except Exception as e:
            logger.error(f"Error in delete_topics: {e}")
            future.set_exception(e)

        return future

    def create_producer(self, topic: str) -> Future:
        """
        Create a producer for the specified topic.

        :param topic: Topic name
        :return: Future that resolves to PulsarBenchmarkProducer
        """
        from .pulsar_benchmark_producer import PulsarBenchmarkProducer

        future = Future()
        try:
            producer = self.client.create_producer(
                topic=topic,
                batching_enabled=self.producer_builder_config['batching_enabled'],
                batching_max_publish_delay_ms=self.producer_builder_config['batching_max_publish_delay_ms'],
                block_if_queue_full=self.producer_builder_config['block_if_queue_full'],
                max_pending_messages=self.producer_builder_config['max_pending_messages'],
                send_timeout_millis=self.producer_builder_config['send_timeout_millis'],
            )

            benchmark_producer = PulsarBenchmarkProducer(producer)
            future.set_result(benchmark_producer)

        except Exception as e:
            logger.error(f"Error creating producer for topic {topic}: {e}")
            future.set_exception(e)

        return future

    def create_consumers(self, consumer_infos: List[Any]) -> Future:
        """
        Create consumers.

        :param consumer_infos: List of consumer information objects
        :return: Future that resolves to list of consumers
        """
        from .pulsar_benchmark_consumer import PulsarBenchmarkConsumer

        future = Future()
        consumers = []

        try:
            consumer_config = self.config.get('consumer', {})
            subscription_type_str = consumer_config.get('subscriptionType', 'Failover')

            subscription_type_map = {
                'Exclusive': pulsar.ConsumerType.Exclusive,
                'Shared': pulsar.ConsumerType.Shared,
                'Failover': pulsar.ConsumerType.Failover,
                'Key_Shared': pulsar.ConsumerType.KeyShared,
            }
            subscription_type = subscription_type_map.get(subscription_type_str, pulsar.ConsumerType.Failover)

            for consumer_info in consumer_infos:
                topic = consumer_info.topic
                subscription = consumer_info.subscription_name
                callback = consumer_info.consumer_callback

                consumer = self.client.subscribe(
                    topic=topic,
                    subscription_name=subscription,
                    consumer_type=subscription_type,
                    receiver_queue_size=consumer_config.get('receiverQueueSize', 1000),
                    message_listener=lambda consumer, msg: self._message_listener(msg, callback),
                )

                benchmark_consumer = PulsarBenchmarkConsumer(consumer)
                consumers.append(benchmark_consumer)

            future.set_result(consumers)

        except Exception as e:
            logger.error(f"Error creating consumers: {e}")
            future.set_exception(e)

        return future

    def _message_listener(self, msg, callback):
        """
        Internal message listener that forwards to callback.

        :param msg: Pulsar message
        :param callback: ConsumerCallback to invoke
        """
        try:
            payload = msg.data()
            publish_timestamp_ms = msg.publish_timestamp()
            callback.message_received(payload, publish_timestamp_ms)
        except Exception as e:
            logger.error(f"Error in message listener: {e}")

    def close(self):
        """Close the driver and release resources."""
        logger.info("Shutting down Pulsar benchmark driver")

        # Close Pulsar client
        if self.client:
            self.client.close()

        # Cleanup namespace if using random namespace mode
        if self.namespace and not self.use_fixed_namespace:
            logger.info(f"Attempting to delete namespace: {self.namespace} (random namespace cleanup)")
            self._delete_namespace_http(self.namespace, self.http_url)
        elif self.namespace and self.use_fixed_namespace:
            logger.info(f"Keeping fixed namespace: {self.namespace} (will be reused in next run)")

        logger.info("Pulsar benchmark driver successfully shut down")

    def _delete_namespace_http(self, namespace: str, http_url: str):
        """
        Delete Pulsar namespace via HTTP admin API.
        This will delete all topics and subscriptions in the namespace.

        :param namespace: Namespace to delete
        :param http_url: HTTP URL for admin API
        """
        if not namespace or not http_url:
            logger.warning("Cannot delete namespace: missing namespace or http_url")
            return

        url = f"{http_url}/admin/v2/namespaces/{namespace}"

        try:
            # First try to delete all topics in the namespace
            topics_url = f"{http_url}/admin/v2/{namespace.replace('/', '/persistent/')}"
            try:
                topics_resp = requests.get(topics_url)
                if topics_resp.status_code == 200:
                    topics = topics_resp.json()
                    if topics:
                        logger.info(f"Found {len(topics)} topics in namespace {namespace}, deleting...")
                        for topic in topics:
                            try:
                                topic_path = topic.replace('://', '/')
                                delete_url = f"{http_url}/admin/v2/{topic_path}"
                                requests.delete(delete_url, params={'force': 'true', 'deleteSchema': 'true'})
                            except Exception as e:
                                logger.debug(f"Error deleting topic {topic}: {e}")
            except Exception as e:
                logger.debug(f"Could not list topics in namespace: {e}")

            # Now try to delete the namespace
            resp = requests.delete(url, params={'force': 'true'})

            if resp.status_code == 204:
                logger.info(f"✅ Successfully deleted namespace: {namespace}")
            elif resp.status_code == 404:
                logger.info(f"⚠️  Namespace not found (already deleted): {namespace}")
            elif resp.status_code == 409:
                logger.warning(f"⚠️  Namespace not empty or has active connections: {namespace}")
                logger.warning(f"   This is normal if topics still have active producers/consumers")
            else:
                logger.warning(f"❌ Failed to delete namespace {namespace}: {resp.status_code} {resp.text}")

        except Exception as e:
            logger.warning(f"❌ Error deleting namespace {namespace}: {e}")
