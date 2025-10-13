# #!/bin/bash

# # 配置参数
# PULSAR_CONTAINER_NAME="pulsar-standalone"
# TENANT_NAME="benchmark"
# NAMESPACE_PREFIX="ns"

# # 1️⃣ 检查 tenant 是否存在
# echo "Checking if tenant '$TENANT_NAME' exists..."
# EXISTS=$(docker exec -it $PULSAR_CONTAINER_NAME bin/pulsar-admin tenants list | grep "$TENANT_NAME")

# if [ -z "$EXISTS" ]; then
#     echo "Tenant '$TENANT_NAME' not found. Creating tenant..."
#     docker exec -it $PULSAR_CONTAINER_NAME bin/pulsar-admin tenants create $TENANT_NAME
# else
#     echo "Tenant '$TENANT_NAME' already exists."
# fi

# # 2️⃣ 检查 namespace 是否存在
# FULL_NAMESPACE="$TENANT_NAME/$NAMESPACE_PREFIX"
# echo "Checking if namespace '$FULL_NAMESPACE' exists..."
# EXISTS_NS=$(docker exec -it $PULSAR_CONTAINER_NAME bin/pulsar-admin namespaces list $TENANT_NAME | grep "$NAMESPACE_PREFIX")

# if [ -z "$EXISTS_NS" ]; then
#     echo "Namespace '$FULL_NAMESPACE' not found. Creating namespace..."
#     docker exec -it $PULSAR_CONTAINER_NAME bin/pulsar-admin namespaces create $FULL_NAMESPACE
# else
#     echo "Namespace '$FULL_NAMESPACE' already exists."
# fi

# echo "✅ Tenant and namespace setup complete."

#!/bin/bash

# Docker 容器名称
PULSAR_CONTAINER="pulsar-standalone"

# Pulsar standalone 配置路径
CONF_PATH="/pulsar/conf/standalone.conf"  # Docker 中默认路径，如果不同请修改

echo "✅ Enabling auto namespace creation in Pulsar standalone..."

# 1️⃣ 修改配置文件，开启 allowAutoNamespaceCreation
docker exec -it $PULSAR_CONTAINER /bin/bash -c "grep -q '^allowAutoNamespaceCreation=true' $CONF_PATH || echo 'allowAutoNamespaceCreation=true' >> $CONF_PATH"
docker exec -it $PULSAR_CONTAINER /bin/bash -c "grep -q '^allowAutoTopicCreation=true' $CONF_PATH || echo 'allowAutoTopicCreation=true' >> $CONF_PATH"
docker exec -it $PULSAR_CONTAINER /bin/bash -c "grep -q '^allowAutoTopicCreationType=partitioned' $CONF_PATH || echo 'allowAutoTopicCreationType=partitioned' >> $CONF_PATH"

# 2️⃣ 重启 Pulsar 容器
echo "🔄 Restarting Pulsar container..."
docker restart $PULSAR_CONTAINER

echo "✅ Pulsar is restarting. Auto namespace creation is enabled."
