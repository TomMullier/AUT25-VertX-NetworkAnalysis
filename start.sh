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

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  -s, --skip-deps   Saute l'étape de build Maven (clean install -DskipTests)
  -q, --quiet       Mode silencieux (réduit la verbosité, Maven -q, logs dans ${LOG_DIR})
  -h, --help        Affiche cette aide

Exemples:
  $0                         # Build + run (verbeux)
  $0 -s                      # Saute le build, lance directement
  $0 --quiet                 # Silencieux, redirige les sorties vers fichiers de logs
  $0 -s -q                   # Saute le build et lance en silencieux
EOF
}


while [[ $# -gt 0 ]]; do
  case "$1" in
    -s|--skip-deps)
      SKIP_DEPS=true
      shift
      ;;
    -q|--quiet)
      QUIET=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      err "Unknown option: $1"
      usage
      exit 2
      ;;
  esac
done



MVN_Q_OPTS=()
if [ "$QUIET" = true ]; then
  MVN_Q_OPTS+=("-q" "--no-transfer-progress")
else
  MVN_Q_OPTS+=("--no-transfer-progress")
fi




# === Title ===
echo " ███████████ ████                           █████   █████                     █████                        ";
echo "▒▒███▒▒▒▒▒▒█▒▒███                          ▒▒███   ▒▒███                     ▒▒███                         ";
echo " ▒███   █ ▒  ▒███   ██████  █████ ███ █████ ▒███    ▒███   ██████  ████████  ███████    ██████  █████ █████";
echo " ▒███████    ▒███  ███▒▒███▒▒███ ▒███▒▒███  ▒███    ▒███  ███▒▒███▒▒███▒▒███▒▒▒███▒    ███▒▒███▒▒███ ▒▒███ ";
echo " ▒███▒▒▒█    ▒███ ▒███ ▒███ ▒███ ▒███ ▒███  ▒▒███   ███  ▒███████  ▒███ ▒▒▒   ▒███    ▒███████  ▒▒▒█████▒  ";
echo " ▒███  ▒     ▒███ ▒███ ▒███ ▒▒███████████    ▒▒▒█████▒   ▒███▒▒▒   ▒███       ▒███ ███▒███▒▒▒    ███▒▒▒███ ";
echo " █████       █████▒▒██████   ▒▒████▒████       ▒▒███     ▒▒██████  █████      ▒▒█████ ▒▒██████  █████ █████";
echo "▒▒▒▒▒       ▒▒▒▒▒  ▒▒▒▒▒▒     ▒▒▒▒ ▒▒▒▒         ▒▒▒       ▒▒▒▒▒▒  ▒▒▒▒▒        ▒▒▒▒▒   ▒▒▒▒▒▒  ▒▒▒▒▒ ▒▒▒▒▒ ";
echo "                                                                                                           ";
echo "                                                                                                           ";
echo "                                                                                                           ";


hr() { printf "${BLUE}%s${NC}\n" "────────────────────────────────────────────────────────────────"; }
section() { printf "${CYAN}◆ %s${NC}\n" "$1"; }
ok() { printf "${GREEN}✔ %s${NC}\n" "$1"; }
warn() { printf "${YELLOW}⚠ %s${NC}\n" "$1"; }
fail() { printf "${RED}✖ %s${NC}\n" "$1" >&2; }

banner() {
  echo -e "${BLUE}┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓${NC}"
  echo -e "${BLUE}┃${NC}  ${CYAN}Automated Setup • AUT25 VertX Network Analysis Project${NC}        ${BLUE}        ┃${NC}"
  echo -e "${BLUE}┃${NC}  ${WHITE}Supports: Ubuntu · Debian · Fedora · CentOS · Arch · OpenSUSE · SUSE${NC} ${BLUE} ┃${NC}"
  echo -e "${BLUE}┃${NC}  ${MAGENTA}By Tom MULLIER${NC}                                             ${BLUE}           ┃${NC}"
  echo -e "${BLUE}┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛${NC}"
  echo ""
}

# En-tête stylé (remplace tes echos initiaux)
banner
if [ "$QUIET" = true ]; then
  warn "Running in quiet mode: output will be minimal."
fi
if [ "$SKIP_DEPS" = true ]; then
  warn "Skipping dependencies installation as per user request."
else
  section "Installing dependencies"
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
echo -e "${BLUE}┃${NC}  ${GREEN}Detected distribution: $DISTRO${NC}"
echo ""

# # Check NDPI installed
# if [ "$SKIP_DEPS" = false ]; then
#     echo -e "${YELLOW}=== Checking for nDPI installation ===${NC}"
#     MISSING_NDPI=false
#     echo ""

#     # Vérifie la présence de la commande ndpiReader
#     if ! command -v ndpiReader &> /dev/null; then
#         echo -e "${RED}❌ nDPI is not installed or ndpiReader not found.${NC}"
#         echo -e "${YELLOW}You can install it manually by following:${NC}"
#         echo -e "${WHITE}  git clone https://github.com/ntop/nDPI.git${NC}"
#         echo -e "${WHITE}  cd nDPI && sudo ./autogen.sh && ./configure && make && sudo make install${NC}"
#         MISSING_NDPI=true
#         exit 1
#     else
#         echo -e "${GREEN}✅ ndpiReader is available.${NC}"
#     fi

#     # Vérifie le dossier des headers
#     if [ ! -d "/usr/include/ndpi" ]; then
#         echo -e "${RED}❌ Missing directory: /usr/include/ndpi/${NC}"
#         echo -e "${YELLOW}Please ensure nDPI headers are installed properly.${NC}"
#         MISSING_NDPI=true
#         exit 1
#     else
#         echo -e "${GREEN}✅ Found: /usr/include/ndpi/${NC}"
#     fi

#     # Check if /usr/local/include/ndpi exists
#     if [ ! -d "/usr/local/include/ndpi" ]; then
#         echo -e "${RED}❌ Missing directory: /usr/local/include/ndpi/${NC}"
#         echo -e "${YELLOW}Please ensure nDPI headers are installed properly in /usr/local/include.${NC}"
#         MISSING_NDPI=true
#         exit 1
#     else
#         echo -e "${GREEN}✅ Found: /usr/local/include/ndpi/${NC}"
#     fi

#     # Check if /usr/include/ndpi exists
#     if [ ! -d "/usr/include/ndpi" ]; then
#         echo -e "${RED}❌ Missing directory: /usr/include/ndpi/${NC}"
#         echo -e "${YELLOW}Please ensure nDPI headers are installed properly in /usr/include.${NC}"
#         MISSING_NDPI=true
#         exit 1
#     else
#         echo -e "${GREEN}✅ Found: /usr/include/ndpi/${NC}"
#     fi


#     # Stoppe l’installation si nDPI est manquant
#     if [ "$MISSING_NDPI" = true ]; then
#         echo ""
#         echo -e "${RED}⚠️ nDPI installation is incomplete. Please install it before continuing.${NC}"
#         echo ""
#         exit 1
#     fi

#     echo -e "${GREEN}=== ✅ nDPI installation verified successfully ===${NC}"
#     echo ""
# fi

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
    #install_package dnf-plugins-core
    #install_package docker-cli
    #install_package containerd
    #install_package docker-compose
    #install_package docker-compose-switch
if [[ "$DISTRO" == "ubuntu" || "$DISTRO" == "debian" ]]; then
    echo -e "${BLUE}Installing Docker CE (Ubuntu/Debian)...${NC}"

    sudo apt-get update
    sudo apt-get install -y ca-certificates curl gnupg lsb-release

    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
        sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg

    echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
    https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) stable" | \
    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

    sudo apt-get update
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

    sudo systemctl enable containerd
    sudo systemctl start containerd
    sudo systemctl enable docker
    sudo systemctl start docker
else
    echo -e "${RED}Docker installation not supported on $DISTRO by this script.${NC}"
    exit 1
fi
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
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}❌ Docker daemon not running.${NC}"
    exit 1
fi
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
        docker compose -f "${compose_file}" up -d "${service_name}" >/dev/null
    fi

    until docker ps --filter "name=${service_name}" --filter "status=running" | grep -q "${service_name}"; do
        [ "$QUIET" = false ] && echo -e "${YELLOW}... waiting for ${service_name} ...${NC}"
        sleep 5
    done
    [ "$QUIET" = false ] && echo -e "${GREEN}✅ ${service_name} is up.${NC}"
}

# === Delete log file ===
LOG_FILE="logs/app.log"
if [ -f "$LOG_FILE" ]; then
    echo -e "${BLUE}┃${NC}  ${BLUE}Deleting old log file: ${YELLOW}$LOG_FILE${NC}"
    rm "$LOG_FILE"
    echo -e "    ${BLUE}┃${NC}  ${GREEN}Old log file deleted.${NC}"
    echo ""
fi

# === Check each service ===
check_and_start_service "zookeeper" "src/main/resources/kafka-docker-compose.yml"
check_and_start_service "kafka" "src/main/resources/kafka-docker-compose.yml"
check_and_start_service "clickhouse" "src/main/resources/kafka-docker-compose.yml"
echo ""

TOPIC_NAME="network-data"
TOPIC_NAME2="network-flows"
NUM_PARTITIONS=500

echo -e "${BLUE}┃${NC}  Checking Kafka...${NC}"

# Check container is running
if ! docker ps --format '{{.Names}}' | grep -q '^kafka$'; then
    echo "❌ Kafka container is not running. Attempting to start it..."
    docker start kafka >/dev/null 2>&1 || {
        echo "❌ Failed to start Kafka container. Please check Docker and try again."
        exit 1
    }
    sleep 5
fi

# Detect kafka-topics command
KAFKA_TOPICS_CMD=$(docker exec kafka sh -c "command -v kafka-topics || command -v kafka-topics.sh")

if [ -z "$KAFKA_TOPICS_CMD" ]; then
  echo "❌ kafka-topics is not found in the Kafka container."
  exit 1
fi

# Wait until Kafka answers
echo -e "    ${BLUE}┃${NC}  Waiting for Kafka to be ready..."
until docker exec kafka sh -c "$KAFKA_TOPICS_CMD --bootstrap-server kafka:9092 --list" >/dev/null 2>&1; do
    echo -e "    ${BLUE}┃${NC}  ⏳ Kafka is not ready yet..."
    sleep 2
done

echo -e "    ${BLUE}┃${NC}  ${GREEN}Kafka is ready.${NC}"
echo ""


# # Reset topics
# echo "=== Resetting Kafka topic: $TOPIC_NAME ==="
# docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic $TOPIC_NAME 2>/dev/null
# docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create \
#     --topic $TOPIC_NAME \
#     --partitions $NUM_PARTITIONS \
#     --replication-factor 1
# echo "✅ Topic $TOPIC_NAME has been reset with $NUM_PARTITIONS partitions."

# echo "=== Resetting Kafka topic: $TOPIC_NAME2 ==="
# docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic $TOPIC_NAME2 2>/dev/null
# docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic $TOPIC_NAME2 --partitions 1 --replication-factor 1
# echo "✅ Topic $TOPIC_NAME2 has been reset."

BROKER="localhost:9092"
DOCKER_CONTAINER="kafka"    # nom du conteneur
NUM_PARTITIONS="${NUM_PARTITIONS:-500}"
TOPIC_NAME="${TOPIC_NAME:-network-data}"
TOPIC_NAME2="${TOPIC_NAME2:-network-flows}"

# Temps d'attente max (secondes) pour la suppression
DELETE_TIMEOUT=120

wait_topic_deleted() {
  local topic="$1"
  local start_ts
  start_ts=$(date +%s)

  echo -e "    ${BLUE}┃${NC}  ⏳ Waiting for complete deletion of topic '$topic'…"
  while true; do
    # 1) Est-ce que le topic existe encore ?
    if docker exec "$DOCKER_CONTAINER" kafka-topics --bootstrap-server "$BROKER" --list 2>/dev/null | grep -Fxq "$topic"; then
      :
    else
      # 2) Vérifie l’état "marked for deletion" via describe; certains brokers ne le listent plus quand MD supprimées
      if docker exec "$DOCKER_CONTAINER" kafka-topics --bootstrap-server "$BROKER" --describe --topic "$topic" 2>&1 \
          | grep -qi "marked for deletion"; then
        :
      else
        echo -e "    ${BLUE}┃${NC}  ${GREEN}Deletion confirmed for '$topic'.${NC}"
        break
      fi
    fi

    # Timeout
    local now
    now=$(date +%s)
    if (( now - start_ts > DELETE_TIMEOUT )); then
      echo -e "⚠️  Timeout reached (${DELETE_TIMEOUT}s) : the topic '$topic' seems to still be deleting."
      echo -e "    Please check the broker/controller logs and disk status. I will continue anyway."
      break
    fi
    sleep 2
  done
}

reset_topic() {
  local topic="$1"
  local partitions="$2"

  echo -e "${BLUE}┃${NC}  Resetting Kafka topic: $topic with $partitions partitions..."
  # Suppression idempotente (ignore l’erreur si non présent)
  docker exec "$DOCKER_CONTAINER" kafka-topics --bootstrap-server "$BROKER" --delete --topic "$topic" >/dev/null 2>&1 || true

  # Attendre la suppression complète
  wait_topic_deleted "$topic"

  # Création idempotente (si option dispo dans ta version Kafka)
  if docker exec "$DOCKER_CONTAINER" kafka-topics --help 2>&1 | grep -q -- "--if-not-exists"; then
    docker exec "$DOCKER_CONTAINER" kafka-topics --bootstrap-server "$BROKER" --create \
      --topic "$topic" \
      --partitions "$partitions" \
      --replication-factor 1 \
      --if-not-exists
  else
    # Fallback sans --if-not-exists : on vérifie avant
    if ! docker exec "$DOCKER_CONTAINER" kafka-topics --bootstrap-server "$BROKER" --list | grep -Fxq "$topic"; then
      docker exec "$DOCKER_CONTAINER" kafka-topics --bootstrap-server "$BROKER" --create \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor 1
    fi
  fi

  echo -e "    ${BLUE}┃${NC}  ${GREEN}Topic $topic has been reset with $partitions partitions.${NC}"
}

# Gestion propre du Ctrl-C pour éviter d’interrompre entre delete/create
trap 'echo; echo "🛑 Stopping..."; exit 130' INT

# Reset topics
reset_topic "$TOPIC_NAME" "$NUM_PARTITIONS"
reset_topic "$TOPIC_NAME2" "1"



# === ClickHouse setup ===
echo -e "${BLUE}┃${NC}  Ensuring ClickHouse database exists...${NC}"
docker exec -i clickhouse clickhouse-client --multiquery < src/main/resources/clickhouse-init/init.sql
echo -e "    ${BLUE}┃${NC}  ${GREEN}ClickHouse database, users and tables are ready.${NC}"
echo ""

# === Maven build ===
if [ "$SKIP_DEPS" = false ]; then
    echo -e "${BLUE}┃${NC}  Building the project with Maven ${NC}"
    mvn clean install -DskipTests "${MVN_Q_OPTS[@]}" >/dev/null 2>&1
    echo -e "    ${BLUE}┃${NC}  ${GREEN}Maven build completed.${NC}"
    echo ""
fi

# === Launch Application ===
echo -e "${GREEN}┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓${NC}"
echo -e "${GREEN}┃${NC}  ${GREEN}Starting the application${NC}                                              ${GREEN}┃${NC}"
echo -e "${GREEN}┃${NC}  ${GREEN}Press Ctrl-C to stop the application${NC}                                  ${GREEN}┃${NC}"
echo -e "${GREEN}┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛${NC}"
mvn -q -Dvertx.disableDebug=true compile vertx:run
echo -e "${BLUE}┃${NC}  ${MAGENTA}Application has stopped${NC}"


# === Autocomplétion pour les flags ===
_autocomplete_flags() {
    local cur opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    # Flags longs + alias courts
    opts="--skip-deps --quiet --help -s -q -h"

    COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
    return 0
}

# Lier la fonction d’autocomplétion au script (chemin relatif ou absolu)
complete -F _autocomplete_flags ./start.s
