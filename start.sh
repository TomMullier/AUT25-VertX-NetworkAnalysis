#!/bin/bash

echo "=== Checking if Kafka and Zookeeper are running ==="

if docker ps --format '{{.Names}}' | grep -qE '(^kafka$|^zookeeper$)'; then
    echo "=== ✅ Kafka and Zookeeper are already running."
else
    echo "=== ❌ Kafka and Zookeeper are not running."
    echo "=== Launching Kafka and Zookeeper ==="
    docker-compose -f src/main/resources/kafka-docker-compose.yml up -d
    # petit délai pour laisser Kafka démarrer
    sleep 5
fi

# sudo docker rm -v kafka zookeeper

TOPIC_NAME="network-data"

echo "=== Resetting Kafka topic: $TOPIC_NAME ==="
docker exec -it kafka \
  kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic $TOPIC_NAME 2>/dev/null

docker exec -it kafka \
  kafka-topics --bootstrap-server localhost:9092 \
  --create --topic $TOPIC_NAME --partitions 1 --replication-factor 1

echo "=== ✅ Topic $TOPIC_NAME has been reset. ==="

echo "=== Starting the application ==="
mvn compile vertx:run
echo "=== Application has stopped ==="