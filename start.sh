#!/bin/bash
#sudo docker stop zookeeper kafka clickhouse && sudo docker rm zookeeper kafka clickhouse
clear

# === Color definitions ===
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color

# === Title ===
echo -e "${CYAN}===================================="
echo -e "===== Network Traffic Analyzer ====="
echo -e "====================================${NC}"
echo ""
echo -e "${WHITE}>> By Tom MULLIER${NC}"
echo ""
echo ""
echo -e "${YELLOW}=== Installing dependencies ===${NC}"


# === Installations ===
echo ""
echo -e "${BLUE}=== Maven installation ===${NC}"
sudo dnf install -y maven
echo ""

echo -e "${BLUE}=== Docker installation ===${NC}"
sudo dnf -y install dnf-plugins-core
sudo dnf install docker-cli containerd -y
sudo dnf install docker-compose -y
sudo dnf install docker-compose-switch -y 
echo ""

echo -e "${BLUE}=== Docker activation and startup ===${NC}"
sudo systemctl enable docker --now
sudo systemctl start docker
echo ""

echo -e "${YELLOW}=== Checking versions ===${NC}"
mvn -v
docker --version
echo ""

echo -e "${GREEN}=== ✅ Installation complete ===${NC}"
echo ""
echo ""

# === Function to check and start a service ===
check_and_start_service() {
    local service_name=$1
    local compose_file=$2

    echo -e "${MAGENTA}=== Checking if ${service_name} is running ===${NC}"

    # Vérifie si le conteneur existe (même arrêté)
    if docker ps -a --format '{{.Names}}' | grep -q "^${service_name}$"; then
        # Vérifie s’il tourne actuellement
        if docker inspect -f '{{.State.Running}}' "${service_name}" 2>/dev/null | grep -q true; then
            echo -e "${GREEN}✅ ${service_name} is already running.${NC}"
            return
        else
            echo -e "${YELLOW}⚠️ ${service_name} exists but is not running. Restarting...${NC}"
            docker start "${service_name}"
        fi
    else
        echo -e "${RED}❌ ${service_name} is not found.${NC}"
        echo -e "${YELLOW}Launching ${service_name}...${NC}"
        docker-compose -f "${compose_file}" up -d "${service_name}"
    fi

    echo -e "${CYAN}Waiting for ${service_name} to be healthy...${NC}"
    # Boucle d’attente jusqu’à ce que le service soit prêt
    until [ "$(docker inspect -f '{{.State.Health.Status}}' "${service_name}" 2>/dev/null || echo 'none')" = "healthy" ] || \
          docker logs "${service_name}" 2>&1 | grep -q "started" || \
          docker ps --filter "name=${service_name}" --filter "status=running" | grep -q "${service_name}"; do
        echo -e "${YELLOW}... still waiting for ${service_name} ...${NC}"
        sleep 5
    done

    echo -e "${GREEN}✅ ${service_name} is up and running.${NC}"
    echo ""
}

# === Check each service ===
check_and_start_service "zookeeper" "src/main/resources/kafka-docker-compose.yml"
check_and_start_service "kafka" "src/main/resources/kafka-docker-compose.yml"
check_and_start_service "clickhouse" "src/main/resources/kafka-docker-compose.yml"
echo ""

# === Kafka Topics ===
TOPIC_NAME="network-data"
TOPIC_NAME2="network-flows"

echo -e "${BLUE}=== Resetting Kafka topic: ${YELLOW}$TOPIC_NAME${NC}"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic $TOPIC_NAME 2>/dev/null
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic $TOPIC_NAME --partitions 1 --replication-factor 1
echo -e "${GREEN}✅ Topic $TOPIC_NAME has been reset.${NC}"

echo -e "${BLUE}=== Resetting Kafka topic: ${YELLOW}$TOPIC_NAME2${NC}"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic $TOPIC_NAME2 2>/dev/null
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic $TOPIC_NAME2 --partitions 1 --replication-factor 1
echo -e "${GREEN}✅ Topic $TOPIC_NAME2 has been reset.${NC}"
echo ""


# === ClickHouse setup ===
echo -e "${BLUE}=== Ensuring ClickHouse database exists ===${NC}"
docker exec -i clickhouse clickhouse-client --multiquery < src/main/resources/clickhouse-init/init.sql
echo -e "${GREEN}✅ ClickHouse database, users and tables are ready.${NC}"
echo ""

# === Maven build ===
echo -e "${BLUE}=== Building the project with Maven ===${NC}"
mvn clean install -DskipTests -q
echo -e "${GREEN}✅ Maven build completed.${NC}"
echo ""

# === Launch Application ===
echo -e "${CYAN}=== Starting the application ===${NC}"
mvn compile vertx:run -q
echo -e "${MAGENTA}=== Application has stopped ===${NC}"
