# FlowVertex

[![Java](https://img.shields.io/badge/Java-21-blue.svg)](https://www.java.com/)
[![Vert.x](https://img.shields.io/badge/Vert.x-4.5.9-green.svg)](https://vertx.io/)
[![Maven](https://img.shields.io/badge/Maven-3.8%2B-orange.svg)](https://maven.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-required-2496ED.svg)](https://www.docker.com/)

## Auteurs et contributeurs
**Auteurs principaux**
-   Tom MULLIER (https://github.com/TomMullier), École de technologie supérieure (ÉTS)
-   Laaziz Lahlou (https://github.com/FlowVertex), École de technologie supérieure (ÉTS)
-   Nadjia Kara (https://www.etsmtl.ca/en/labs/imagin-lab), École de technologie supérieure (ÉTS)

## Contributeurs actuels
- Abdelillah Serghine (https://github.com/serghine-abdelillah), École supérieure en informatique 08 Mai 1945 de Sidi Bel Abbès (ESI-SBA)

FlowVertex is a real-time network analysis platform built with Vert.x. It provides a robust pipeline for ingesting, processing, and analyzing network traffic.

## Core Pipeline

1. **Ingestion**: Captures network packets via PCAP files or live network interfaces.
2. **Raw Data Stream**: Publishes raw packets to a Kafka topic (`network-data`).
3. **Aggregation & Enrichment**: Aggregates packets into enriched flows, adding metadata such as GeoIP, DNS, WHOIS, traffic features, and predictions.
4. **Flow Stream**: Publishes the enriched flows to a Kafka topic (`network-flows`).
5. **Persistence**: Stores all data (packets, flows, and metrics) in ClickHouse for fast, analytical querying.
6. **Web Interface**: Exposes an HTTP API and real-time WebSocket stream to power the frontend UI.

## Key Features

- **Multi-Mode Ingestion**: Run in `realtime` (live capture), `pcap` (replay), or `pcap-instant` modes.
- **High-Throughput Processing**: Built on Vert.x and Kafka for scalable event-driven architecture.
- **Rich Enrichment**: Integrates with external data sources and nDPI for deep packet inspection.
- **Observability**: Tracks system metrics (CPU, RAM) and processing rates in real-time.
- **MLOps & Real-Time Prediction**: Built-in Machine Learning module (XGBoost via JPMML) for real-time flow intrusion detection, complete with an MLOps Monitor dashboard to track performance, predictions, and feature drift.
- **Authentication**: Built-in user signup/login system backed by SQLite and BCrypt.

## Architecture & Verticles

The application is built around the Vert.x actor model, using multiple distinct "Verticles" to separate concerns and handle concurrency efficiently:

- **`Main`**: The global orchestrator handling the interactive menu and the deployment of all other verticles.
- **`IngestionVerticle`**: Responsible for packet capture (live or PCAP replay) and publishing raw packets to Kafka.
- **`FlowAggregatorVerticle`**: Handles the aggregation of packets into flows, enriches them, executes the ML model for real-time intrusion detection, and publishes to the flows topic.
- **`ClickHousePacketVerticle` & `ClickHouseFlowsVerticle`**: Consumers that persist the `network-data` and `network-flows` topics into ClickHouse.
- **`WebServerVerticle`**: Serves the HTTP REST API, the static frontend files, and manages real-time WebSocket communication.
- **`MetricsVerticle` & `SystemMetricsVerticle`**: Collect and flush system resources (CPU/RAM) and processing metrics to ClickHouse.
- **`BenchmarkVerticle`**: Extracts benchmark data into CSV format from Kafka for comparative analysis.
- **`PcapCoordinatorVerticle`**: Coordinates the end-of-processing state when reading from PCAP files.

## Prerequisites

- **Linux** (Ubuntu/Debian recommended)
- **Java 21**
- **Maven 3.8+**
- **Docker & Docker Compose** (for Kafka, Zookeeper, ClickHouse)

## Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/TomMullier/AUT25-VertX-NetworkAnalysis.git
cd AUT25-VertX-NetworkAnalysis
```

### 2. Run the Full Setup

```bash
chmod +x start.sh
./start.sh
```

**Available Options:**

```bash
./start.sh --skip-deps --quiet
```

- `--skip-deps` (`-s`): Skips the installation of system dependencies and the `mvn clean install -DskipTests` step.
- `--quiet` (`-q`): Runs in quiet mode.

**This script will:**

1. Check/install the dependencies.
2. Start Zookeeper, Kafka, and ClickHouse.
3. Reset the `network-data` and `network-flows` topics.
4. Execute `init.sql` on ClickHouse.
5. Launch the application using `mvn -q -Dvertx.disableDebug=true compile vertx:run`.

### 3. Manual Launch (Without Script)

If you want to separate the steps:

```bash
docker compose -f src/main/resources/kafka-docker-compose.yml up -d
mvn clean install -DskipTests
mvn compile vertx:run
```

## How to Use

Once the application is running, access the web interface at:
**http://localhost:8080**

### 1. Authentication
You will be redirected to the login page. First-time users can sign up to create an account. The authentication system uses secure session tokens and BCrypt password hashing.

### 2. Runtime Configuration

File: `src/main/resources/config.json`

Important fields:
- `http.port`: HTTP/WebSocket port (default `8080`).
- `store`: enables/disables ClickHouse writing (`"true"`/`"false"`).
- `mode`:
  - `menu`: interactive menu at startup.
  - `pcap`, `pcap-instant`, `realtime`: direct execution.
- `pcap.file-path`: PCAP file to read.
- `pcap.delay`: temporal replay (`true`) vs immediate reading (`false`).
- `realtime.interface`: network interface for live capture.

## Available HTTP APIs

Routes exposed by `WebServerVerticle`:

- `GET /api/settings`: reads current runtime config.
- `POST /api/settings`: updates config (ingestion + flow timeouts).
- `GET /api/getIngestionMethod`: returns active method.
- `GET /api/pcapInfo`: lists available `.pcap` files + active file.
- `GET /api/networkInfo`: lists active network interfaces + selected interface.
- `GET /api/checkFileExists?file=<name>`: verifies presence of a file in `src/main/resources/data`.

## WebSocket

WebSocket Endpoint: `ws://localhost:8080/` (same port as HTTP).

Live broadcast messages (`type` field):
- `flow`
- `currentFlow`
- `malformedPacket`
- system/processing metrics (`metrics.core`)

## ClickHouse Database

SQL Script: `src/main/resources/clickhouse-init/init.sql`

Elements created:
- Database `network_analysis`
- Tables `network_data`, `network_flows`, `metrics`
- User `admin` / password `admin`

## Benchmark & Validation

The `_tests/benchmark/` folder contains 3 phases:
- `01_phase`: packet comparison (`tshark`, `scapy`, CSV).
- `02_phase`: flow comparison (Vert.x vs NFStream).
- `03_phase`: flow features comparison.

Useful scripts:
- `_tests/benchmark/01_phase/reference_capture.sh`
- `_tests/benchmark/01_phase/tshark_analysis.sh`
- `_tests/benchmark/01_phase/scapy_analysis.py`
- `_tests/benchmark/02_phase/nfstream_gen.py`
- `_tests/benchmark/03_phase/nfstream_features.py`

*Note: `_tests/benchmark/02_phase/env/` contains a local Python environment.*

## JNI / nDPI (Optional)

Files:
- `native/ndpi_jni.c`
- `native/c_compile.sh`
- `native/libndpi_jni.so`
- `src/main/java/com/aut25/vertx/utils/NDPIWrapper.java`

Compilation (inside `native/`):
```bash
chmod +x c_compile.sh
./c_compile.sh
```

**Warning:**
- `NDPIWrapper` currently loads a library with a local absolute path.
- Adapt this path according to your machine if necessary.

## Logs & Observability

- Logs configuration: `src/main/resources/logback.xml`.
- Metrics sent to ClickHouse `metrics` table.
- Docker Logs:
```bash
docker logs kafka
docker logs clickhouse
```

## Stopping & Cleanup

Stop services:
```bash
docker compose -f src/main/resources/kafka-docker-compose.yml down
```

Or manually:
```bash
sudo docker stop zookeeper kafka clickhouse
sudo docker rm zookeeper kafka clickhouse
```

## Quick Troubleshooting

- Verify Java/Maven:
```bash
java -version
mvn -v
```

- Verify containers:
```bash
docker ps
```

- Verify API port:
```bash
curl http://localhost:8080/api/settings
```

- If a Kafka topic is having issues, restart `./start.sh` for a clean reset.


## Remerciements
Nous remercions Anes Abdennebi (https://www.linkedin.com/in/abdennebi-anes/), doctorant a l'ÉTS pour nous avoir fourni le script (tester.py + README_tester_script.md) pour la génération d'attaques.
## Licence
MIT. Veuillez lire le fichier LICENSE.
