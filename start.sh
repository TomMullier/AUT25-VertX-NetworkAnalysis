#!/bin/bash

echo "=== Checking if Kafka and Zookeeper are running ==="

if docker ps --format '{{.Names}}' | grep -qE '(^kafka$|^zookeeper$)'; then
    echo "=== ✅ Kafka and Zookeeper are already running."
else
        echo "=== ❌ Kafka and Zookeeper are not running."
    echo "=== Launching Kafka and Zookeeper ==="
    docker-compose -f src/main/resources/kafka-docker-compose.yml up -d
fi

echo "=== Starting the application ==="
mvn compile vertx:run

