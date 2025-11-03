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
echo " __    __             __                                        __       ";
echo "|  \\  |  \\           |  \\                                      |  \\      ";
echo "| \$\$\\ | \$\$  ______  _| \$\$_    __   __   __   ______    ______  | \$\$   __ ";
echo "| \$\$\$\\| \$\$ /      \\|   \$\$ \\  |  \\ |  \\ |  \\ /      \\  /      \\ | \$\$  /  \\";
echo "| \$\$\$\$\\ \$\$|  \$\$\$\$\$\$\\\\\$\$\$\$\$\$  | \$\$ | \$\$ | \$\$|  \$\$\$\$\$\$\\|  \$\$\$\$\$\$\\| \$\$_/  \$\$";
echo "| \$\$\\\$\$ \$\$| \$\$    \$\$ | \$\$ __ | \$\$ | \$\$ | \$\$| \$\$  | \$\$| \$\$   \\\$\$| \$\$   \$\$ ";
echo "| \$\$ \\\$\$\$\$| \$\$\$\$\$\$\$\$ | \$\$|  \\| \$\$_/ \$\$_/ \$\$| \$\$__/ \$\$| \$\$      | \$\$\$\$\$\$\\ ";
echo "| \$\$  \\\$\$\$ \\\$\$     \\  \\\$\$  \$\$ \\\$\$   \$\$   \$\$ \\\$\$    \$\$| \$\$      | \$\$  \\\$\$\\";
echo " \\\$\$   \\\$\$  \\\$\$\$\$\$\$\$   \\\$\$\$\$   \\\$\$\$\$\$\\\$\$\$\$   \\\$\$\$\$\$\$  \\\$\$       \\\$\$   \\\$\$";
echo " ________                    ______    ______   __           ";
echo "|        \\                  /      \\  /      \\ |  \\          ";
echo " \\\$\$\$\$\$\$\$\$______   ______  |  \$\$\$\$\$\$\\|  \$\$\$\$\$\$\\ \\\$\$  _______ ";
echo "   | \$\$  /      \\ |      \\ | \$\$_  \\\$\$| \$\$_  \\\$\$|  \\ /       \\";
echo "   | \$\$ |  \$\$\$\$\$\$\\ \\\$\$\$\$\$\$\\| \$\$ \\    | \$\$ \\    | \$\$|  \$\$\$\$\$\$\$";
echo "   | \$\$ | \$\$   \\\$\$/      \$\$| \$\$\$\$    | \$\$\$\$    | \$\$| \$\$      ";
echo "   | \$\$ | \$\$     |  \$\$\$\$\$\$\$| \$\$      | \$\$      | \$\$| \$\$_____ ";
echo "   | \$\$ | \$\$      \\\$\$    \$\$| \$\$      | \$\$      | \$\$ \\\$\$     \\";
echo "    \\\$\$  \\\$\$       \\\$\$\$\$\$\$\$ \\\$\$       \\\$\$       \\\$\$  \\\$\$\$\$\$\$\$";
echo "  ______                       __                                         ";
echo " /      \\                     |  \\                                        ";
echo "|  \$\$\$\$\$\$\\ _______    ______  | \$\$ __    __  ________   ______    ______  ";
echo "| \$\$__| \$\$|       \\  |      \\ | \$\$|  \\  |  \\|        \\ /      \\  /      \\ ";
echo "| \$\$    \$\$| \$\$\$\$\$\$\$\\  \\\$\$\$\$\$\$\\| \$\$| \$\$  | \$\$ \\\$\$\$\$\$\$\$\$|  \$\$\$\$\$\$\\|  \$\$\$\$\$\$\\";
echo "| \$\$\$\$\$\$\$\$| \$\$  | \$\$ /      \$\$| \$\$| \$\$  | \$\$  /    \$\$ | \$\$    \$\$| \$\$   \\\$\$";
echo "| \$\$  | \$\$| \$\$  | \$\$|  \$\$\$\$\$\$\$| \$\$| \$\$__/ \$\$ /  \$\$\$\$_ | \$\$\$\$\$\$\$\$| \$\$      ";
echo "| \$\$  | \$\$| \$\$  | \$\$ \\\$\$    \$\$| \$\$ \\\$\$    \$\$|  \$\$    \\ \\\$\$     \\| \$\$      ";
echo " \\\$\$   \\\$\$ \\\$\$   \\\$\$  \\\$\$\$\$\$\$\$ \\\$\$ _\\\$\$\$\$\$\$\$ \\\$\$\$\$\$\$\$\$  \\\$\$\$\$\$\$\$ \\\$\$      ";
echo "                                  |  \\__| \$\$                              ";
echo "                                   \\\$\$    \$\$                              ";
echo "                                    \\\$\$\$\$\$\$                               ";   

echo ""
echo -e "${CYAN}=== Automated Setup Script for AUT25 VertX Network Analysis Project ===${NC}"
echo -e "${CYAN}=== Supports Ubuntu, Debian, Fedora, CentOS, Arch, OpenSUSE, and SUSE ===${NC}"
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

# Check NDPI installed
if [ "$SKIP_DEPS" = false ]; then
    echo -e "${YELLOW}=== Checking for nDPI installation ===${NC}"
    MISSING_NDPI=false
    echo ""

    # Vérifie la présence de la commande ndpiReader
    if ! command -v ndpiReader &> /dev/null; then
        echo -e "${RED}❌ nDPI is not installed or ndpiReader not found.${NC}"
        echo -e "${YELLOW}You can install it manually by following:${NC}"
        echo -e "${WHITE}  git clone https://github.com/ntop/nDPI.git${NC}"
        echo -e "${WHITE}  cd nDPI && sudo ./autogen.sh && ./configure && make && sudo make install${NC}"
        MISSING_NDPI=true
        exit 1
    else
        echo -e "${GREEN}✅ ndpiReader is available.${NC}"
    fi

    # Vérifie le dossier des headers
    if [ ! -d "/usr/include/ndpi" ]; then
        echo -e "${RED}❌ Missing directory: /usr/include/ndpi/${NC}"
        echo -e "${YELLOW}Please ensure nDPI headers are installed properly.${NC}"
        MISSING_NDPI=true
        exit 1
    else
        echo -e "${GREEN}✅ Found: /usr/include/ndpi/${NC}"
    fi

    # Check if /usr/local/include/ndpi exists
    if [ ! -d "/usr/local/include/ndpi" ]; then
        echo -e "${RED}❌ Missing directory: /usr/local/include/ndpi/${NC}"
        echo -e "${YELLOW}Please ensure nDPI headers are installed properly in /usr/local/include.${NC}"
        MISSING_NDPI=true
        exit 1
    else
        echo -e "${GREEN}✅ Found: /usr/local/include/ndpi/${NC}"
    fi

    # Check if /usr/include/ndpi exists
    if [ ! -d "/usr/include/ndpi" ]; then
        echo -e "${RED}❌ Missing directory: /usr/include/ndpi/${NC}"
        echo -e "${YELLOW}Please ensure nDPI headers are installed properly in /usr/include.${NC}"
        MISSING_NDPI=true
        exit 1
    else
        echo -e "${GREEN}✅ Found: /usr/include/ndpi/${NC}"
    fi


    # Stoppe l’installation si nDPI est manquant
    if [ "$MISSING_NDPI" = true ]; then
        echo ""
        echo -e "${RED}⚠️ nDPI installation is incomplete. Please install it before continuing.${NC}"
        echo ""
        exit 1
    fi

    echo -e "${GREEN}=== ✅ nDPI installation verified successfully ===${NC}"
    echo ""
fi

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

# === Delete log file ===
LOG_FILE="logs/app.log"
if [ -f "$LOG_FILE" ]; then
    echo -e "${BLUE}=== Deleting old log file: ${YELLOW}$LOG_FILE${NC}"
    rm "$LOG_FILE"
    echo -e "${GREEN}✅ Old log file deleted.${NC}"
    echo ""
fi

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
if [ "$SKIP_DEPS" = false ]; then
    echo -e "${BLUE}=== Building the project with Maven ===${NC}"
    mvn clean install -DskipTests -q
    echo -e "${GREEN}✅ Maven build completed.${NC}"
    echo ""
fi

# === Launch Application ===
echo -e "${CYAN}=== Starting the application ===${NC}"
mvn compile vertx:run 
echo -e "${MAGENTA}=== Application has stopped ===${NC}"

# === Autocompletion for flags ===
_autocomplete_flags() {
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    opts="--skip-deps --quiet"

    # Si tu veux plus tard ajouter d’autres flags, ajoute-les ici :
    # opts="--skip-deps --quiet --no-cache --force"

    COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
    return 0
}

# Lie la fonction d’autocomplétion au script
complete -F _autocomplete_flags ./start.sh
