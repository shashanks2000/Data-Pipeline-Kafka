#!/bin/bash

# Start Zookeeper (Run in a separate terminal)
zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties

# Start Kafka Server (Run in a separate terminal)
kafka-server-start /opt/homebrew/etc/kafka/server.properties

# Create a topic
kafka-topics --create --topic api-data-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# List all topics
kafka-topics --list --bootstrap-server localhost:9092

# Describe a topic
kafka-topics --describe --topic api-data-topic --bootstrap-server localhost:9092

# Start console producer (Run in a separate terminal)
kafka-console-producer --topic api-data-topic --bootstrap-server localhost:9092

# Start console consumer (Run in a separate terminal)
kafka-console-consumer --topic api-data-topic --bootstrap-server localhost:9092 --from-beginning

# List consumer groups
kafka-consumer-groups --list --bootstrap-server localhost:9092

# Describe a consumer group
kafka-consumer-groups --describe --group api-data-consumer-group --bootstrap-server localhost:9092

# Delete a topic
kafka-topics --delete --topic api-data-topic --bootstrap-server localhost:9092

# Check ports in use
lsof -i :9092  # Check if Kafka port is in use
lsof -i :2181  # Check if Zookeeper port is in use

# Stop Kafka and Zookeeper
kafka-server-stop
zookeeper-server-stop

# Clear Kafka logs if needed (be careful with this)
rm -rf /tmp/kafka-logs/
rm -rf /tmp/zookeeper/

# Monitor Kafka logs
tail -f /opt/homebrew/var/log/kafka/kafka.log



#### Quick Start Steps (copy these commands one by one) ####
# Ensure you have Kafka and Zookeeper installed and configured correctly.

# 1. Start Zookeeper (Terminal 1)
zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties

# 2. Start Kafka (Terminal 2)
kafka-server-start /opt/homebrew/etc/kafka/server.properties

# 3. Create Topic (Terminal 3)
kafka-topics --create --topic api-data-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# 4. Verify Topic
kafka-topics --list --bootstrap-server localhost:9092

# 5. Run your Python application
python main.py



#### To test without your Python application ####
# Terminal 3 - Producer
kafka-console-producer --topic api-data-topic --bootstrap-server localhost:9092

# Terminal 4 - Consumer
kafka-console-consumer --topic api-data-topic --bootstrap-server localhost:9092 --from-beginning




#### Cleanup Commands ####

# Stop everything
kafka-server-stop
zookeeper-server-stop

# Clear logs (if needed)
rm -rf /tmp/kafka-logs/
rm -rf /tmp/zookeeper/