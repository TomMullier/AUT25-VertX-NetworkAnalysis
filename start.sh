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

# === Parse arguments ===
SKIP_DEPS=false
QUIET=false

for arg in "$@"; do
    case $arg in
        --skip-deps)
            SKIP_DEPS=true
            shift
            ;;
        --quiet)
            QUIET=true
            shift
            ;;
    esac
done

# === Title ===
echo -e "${CYAN}===================================="
echo -e "===== Network Traffic Analyzer ====="
echo -e "====================================${NC}"
echo ""
echo -e "${WHITE}>> By Tom MULLIER${NC}"
echo ""
echo ""
echo -e "${YELLOW}=== Installing dependencies ===${NC}"

# === Minimal mode check ===
if [ "$QUIET" = true ]; then
    echo -e "${YELLOW}Mode silencieux activé (affichage minimal).${NC}"
fi
if [ "$SKIP_DEPS" = true ]; then
    echo -e "${YELLOW}Skipping dependencies installation as per user request.${NC}"
else
    echo -e "${YELLOW}Dependencies installation will proceed.${NC}"
fi
echo ""

# === Detect OS function ===
detect_distro() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        DISTRO=$ID
    else
        echo -e "${RED}❌ Cannot detect the operating system.${NC}"
        exit 1
    fi
}
detect_distro
echo -e "${GREEN}>> Detected distribution: $DISTRO${NC}"
echo ""

# === Install packages function ===
install_package() {
    PACKAGE=$1
    if [ "$QUIET" = false ]; then
        echo -e "${BLUE}Installing ${PACKAGE}...${NC}"
    fi

    case "$DISTRO" in
        ubuntu|debian)
            sudo apt-get update -qq >/dev/null 2>&1
            sudo apt-get install -y -qq $PACKAGE >/dev/null 2>&1
            ;;
        fedora|centos|rhel)
            sudo dnf install -y -q $PACKAGE >/dev/null 2>&1
            ;;
        arch)
            sudo pacman -Syu --noconfirm $PACKAGE >/dev/null 2>&1
            ;;
        opensuse*|suse)
            sudo zypper install -y -q $PACKAGE >/dev/null 2>&1
            ;;
        *)
            echo -e "${RED}❌ Distribution $DISTRO not supported.${NC}"
            exit 1
            ;;
    esac
}


# === Installations ===
if [ "$SKIP_DEPS" = false ]; then
    echo -e "${YELLOW}=== Installing dependencies ===${NC}"
    if [ "$QUIET" = false ]; then
        echo ""
        echo -e "${BLUE}=== Maven installation ===${NC}"
    fi
    install_package maven
    if [ "$QUIET" = false ]; then
        echo ""
        echo -e "${BLUE}=== Docker installation ===${NC}"
    fi
    install_package dnf-plugins-core
    install_package docker-cli
    install_package containerd
    install_package docker-compose
    install_package docker-compose-switch
    if [ "$QUIET" = false ]; then
        echo ""
        echo -e "${BLUE}=== Docker activation and startup ===${NC}"
    fi
    sudo systemctl enable docker --now
    sudo systemctl start docker
    if [ "$QUIET" = false ]; then
        echo ""
        echo -e "${YELLOW}=== Checking versions ===${NC}"
        mvn -v
        docker --version
        echo ""
    fi
    echo -e "${GREEN}=== ✅ Installation complete ===${NC}"
    echo ""
    echo ""
fi



# === Function to check and start a service ===
check_and_start_service() {
    local service_name=$1
    local compose_file=$2

    if [ "$QUIET" = false ]; then
        echo -e "${MAGENTA}=== Checking ${service_name} ===${NC}"
    fi

    if docker ps -a --format '{{.Names}}' | grep -q "^${service_name}$"; then
        if docker inspect -f '{{.State.Running}}' "${service_name}" 2>/dev/null | grep -q true; then
            [ "$QUIET" = false ] && echo -e "${GREEN}✅ ${service_name} is already running.${NC}"
            return
        else
            [ "$QUIET" = false ] && echo -e "${YELLOW}⚠️ Restarting ${service_name}...${NC}"
            docker start "${service_name}" >/dev/null
        fi
    else
        [ "$QUIET" = false ] && echo -e "${YELLOW}Launching ${service_name}...${NC}"
        docker-compose -f "${compose_file}" up -d "${service_name}" >/dev/null
    fi

    until docker ps --filter "name=${service_name}" --filter "status=running" | grep -q "${service_name}"; do
        [ "$QUIET" = false ] && echo -e "${YELLOW}... waiting for ${service_name} ...${NC}"
        sleep 5
    done
    [ "$QUIET" = false ] && echo -e "${GREEN}✅ ${service_name} is up.${NC}"
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
