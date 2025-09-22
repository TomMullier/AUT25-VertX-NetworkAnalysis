#!/bin/bash
clear
echo "=== Checking if Zookeeper is running ==="
if docker ps --format '{{.Names}}' | grep -q '^zookeeper$' ; then
    echo "=== ✅ Zookeeper is already running."
else
    echo "=== ❌ Zookeeper is not running."
    echo "=== Launching Zookeeper ==="
    docker-compose -f src/main/resources/kafka-docker-compose.yml up -d zookeeper
    sleep 5
fi

echo "=== Checking if Kafka is running ==="
if docker ps --format '{{.Names}}' | grep -q '^kafka$' ; then
    echo "=== ✅ Kafka is already running."
else
    echo "=== ❌ Kafka is not running."
    echo "=== Launching Kafka ==="
    docker-compose -f src/main/resources/kafka-docker-compose.yml up -d kafka
    sleep 5
fi

echo "=== Checking if ClickHouse is running ==="
if docker ps --format '{{.Names}}' | grep -q '^clickhouse$' ; then
    echo "=== ✅ ClickHouse is already running."
else
    echo "=== ❌ ClickHouse is not running."
    echo "=== Launching ClickHouse ==="
    docker-compose -f src/main/resources/kafka-docker-compose.yml up -d clickhouse
    sleep 5
fi

TOPIC_NAME="network-data"

echo "=== Resetting Kafka topic: $TOPIC_NAME ==="
docker exec kafka \
  kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic $TOPIC_NAME 2>/dev/null

docker exec kafka \
  kafka-topics --bootstrap-server localhost:9092 \
  --create --topic $TOPIC_NAME --partitions 1 --replication-factor 1

echo "=== ✅ Topic $TOPIC_NAME has been reset. ==="

echo "=== Ensuring ClickHouse database exists ==="
docker exec clickhouse clickhouse-client --query="CREATE DATABASE IF NOT EXISTS network_analysis;"
docker exec clickhouse clickhouse-client --query="CREATE USER admin IDENTIFIED WITH PLAINTEXT_PASSWORD BY 'admin';"
docker exec clickhouse clickhouse-client --query="GRANT ALL ON network_analysis.* TO admin;" 
echo "=== ✅ ClickHouse database and user are set up. ==="

echo "=== Starting the application ==="
mvn compile vertx:run
echo "=== Application has stopped ==="
