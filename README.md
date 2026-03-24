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

## Etudiants (contributeurs) actuels
- Tom MULLIER (https://github.com/TomMullier), École de technologie supérieure (ÉTS)
- Abdelillah Serghine (https://github.com/serghine-abdelillah), École supérieure en informatique 08 Mai 1945 de Sidi Bel Abbès (ESI-SBA)

## Presentation

Ce projet est une plateforme d'analyse reseau en temps reel construite avec Vert.x.

Le pipeline principal est le suivant:

1. Ingestion de paquets reseau (PCAP ou interface reseau live).
2. Publication dans Kafka (`network-data`).
3. Aggregation en flux enrichis (GeoIP, DNS, WHOIS, features de trafic, prediction).
4. Publication des flux dans Kafka (`network-flows`).
5. Persistance dans ClickHouse (`network_data`, `network_flows`, `metrics`).
6. Exposition Web via API HTTP + WebSocket (UI dans `src/main/resources/webroot`).

Le projet contient aussi des scripts de benchmark Python et shell pour comparer les resultats Vert.x avec tshark, Scapy et NFStream.

## Fonctionnalites Principales

- Ingestion multi-mode: `pcap-instant`, `pcap`, `realtime`.
- Pipeline Kafka a haut debit (`network-data`, `network-flows`).
- Agrgation de flux avec un grand nombre de workers (`FlowAggregatorVerticle`).
- Enrichissement des flux (GeoIP, DNS, WHOIS).
- Export et stockage ClickHouse (paquets, flux, metriques).
- Mesure des performances internes (CPU, RAM, taux de traitement).
- API REST de configuration/inspection.
- Streaming temps reel par WebSocket vers le front.
- Support JNI/nDPI (`native/ndpi_jni.c`, `libndpi_jni.so`).

## Architecture Technique

### Services Externes

- Zookeeper
- Kafka (Confluent image)
- ClickHouse

Definition Docker Compose: `src/main/resources/kafka-docker-compose.yml`.

### Verticles Principaux

- `com.aut25.vertx.Main`: orchestrateur global, menu interactif, deployment.
- `com.aut25.vertx.IngestionVerticle`: capture/replay de paquets et envoi Kafka.
- `com.aut25.vertx.FlowAggregatorVerticle`: aggregation, enrichissement, publication des flux.
- `com.aut25.vertx.ClickHousePacketVerticle`: persistance topic `network-data`.
- `com.aut25.vertx.ClickHouseFlowsVerticle`: persistance topic `network-flows`.
- `com.aut25.vertx.api.WebServerVerticle`: API HTTP + static files + WebSocket.
- `com.aut25.vertx.MetricsVerticle`: bufferisation/flushing des metriques vers ClickHouse.
- `com.aut25.vertx.SystemMetricsVerticle`: collecte CPU/RAM periodique.
- `com.aut25.vertx.BenchmarkVerticle`: extraction benchmark CSV depuis Kafka.
- `com.aut25.vertx.PcapCoordinatorVerticle`: coordination de fin de traitement PCAP.

### Topics Kafka

- `network-data`: paquets bruts/normalises en JSON.
- `network-flows`: flux agreges et enrichis.

Le script `start.sh` reinitialise les topics au demarrage.

## Arborescence Utile

- `src/main/java/com/aut25/vertx/`: coeur applicatif Vert.x.
- `src/main/java/com/aut25/vertx/api/routes/`: routes API.
- `src/main/resources/config.json`: configuration runtime principale.
- `src/main/resources/webroot/`: front-end static.
- `src/main/resources/clickhouse-init/init.sql`: schema + user ClickHouse.
- `src/main/resources/data/`: jeux de donnees locaux (JSON/PCAP).
- `native/`: code JNI nDPI et script de compilation.
- `_tests/benchmark/`: scripts de benchmark comparatif.
- `_tests/flux/`: scripts de validation de flux/malformed packets.

## Prerequis

- Linux (Ubuntu/Debian officiellement gerees dans `start.sh`, autres distributions partiellement prevues).
- Java 21.
- Maven 3.8+.
- Docker + Docker Compose plugin.
- (Optionnel) nDPI + headers de dev pour la partie JNI.
- Droits `sudo` pour installation/gestion Docker via script.

## Installation Rapide

### 1. Cloner le depot

```bash
git clone https://github.com/TomMullier/AUT25-VertX-NetworkAnalysis.git
cd AUT25-VertX-NetworkAnalysis
```

### 2. Lancer le setup complet

```bash
chmod +x start.sh
./start.sh
```

Options disponibles:

```bash
./start.sh --skip-deps --quiet
```

- `--skip-deps` (`-s`): saute l'installation des dependances systeme et le `mvn clean install -DskipTests`.
- `--quiet` (`-q`): mode silencieux.

Ce script:

1. Verifie/installe les dependances.
2. Demarre `zookeeper`, `kafka`, `clickhouse`.
3. Reinitialise `network-data` et `network-flows`.
4. Execute `init.sql` sur ClickHouse.
5. Lance l'application avec `mvn -q -Dvertx.disableDebug=true compile vertx:run`.

## Lancement Manuel (Sans Script)

Si vous voulez separer les etapes:

```bash
docker compose -f src/main/resources/kafka-docker-compose.yml up -d
mvn clean install -DskipTests
mvn compile vertx:run
```

## Configuration Runtime

Fichier: `src/main/resources/config.json`

Champs importants:

- `http.port`: port HTTP/WebSocket (default `8080`).
- `store`: active/desactive l'ecriture ClickHouse (`"true"`/`"false"`).
- `mode`:
- `menu`: menu interactif au demarrage.
- `pcap`, `pcap-instant`, `realtime`: execution directe.
- `pcap.file-path`: fichier PCAP a lire.
- `pcap.delay`: replay temporel (`true`) vs lecture immediate (`false`).
- `realtime.interface`: interface reseau pour capture live.

## API HTTP Disponibles

Routes exposees par `WebServerVerticle`:

- `GET /api/settings`: lit la config runtime actuelle.
- `POST /api/settings`: met a jour la config (ingestion + timeouts de flux).
- `GET /api/getIngestionMethod`: retourne la methode active.
- `GET /api/pcapInfo`: liste les fichiers `.pcap` disponibles + fichier actif.
- `GET /api/networkInfo`: liste des interfaces reseau actives + interface selectionnee.
- `GET /api/checkFileExists?file=<nom>`: verifie la presence d'un fichier dans `src/main/resources/data`.

## WebSocket

Endpoint WebSocket: `ws://localhost:8080/` (meme port que HTTP).

Messages diffuses en direct (champ `type`):

- `flow`
- `currentFlow`
- `malformedPacket`
- metriques systeme/traitement (`metrics.core`)

## Base ClickHouse

Script SQL: `src/main/resources/clickhouse-init/init.sql`

Elements crees:

- Base `network_analysis`
- Tables `network_data`, `network_flows`, `metrics`
- Utilisateur `admin` / mot de passe `admin`

## Benchmark Et Validation

Le dossier `_tests/benchmark/` contient 3 phases:

- `01_phase`: comparaison paquets (`tshark`, `scapy`, CSV).
- `02_phase`: comparaison de flux (Vert.x vs NFStream).
- `03_phase`: comparaison de features de flux.

Scripts utiles:

- `_tests/benchmark/01_phase/reference_capture.sh`
- `_tests/benchmark/01_phase/tshark_analysis.sh`
- `_tests/benchmark/01_phase/scapy_analysis.py`
- `_tests/benchmark/02_phase/nfstream_gen.py`
- `_tests/benchmark/03_phase/nfstream_features.py`

Note: `_tests/benchmark/02_phase/env/` contient un environnement Python local.

## JNI / nDPI (Optionnel)

Fichiers:

- `native/ndpi_jni.c`
- `native/c_compile.sh`
- `native/libndpi_jni.so`
- `src/main/java/com/aut25/vertx/utils/NDPIWrapper.java`

Compilation (dans `native/`):

```bash
chmod +x c_compile.sh
./c_compile.sh
```

Attention:

- `NDPIWrapper` charge actuellement une librairie avec un chemin absolu local.
- Adaptez ce chemin selon votre machine si necessaire.

## Logs Et Observabilite

- Configuration logs: `src/main/resources/logback.xml`.
- Metriques envoyees dans la table ClickHouse `metrics`.
- Logs Docker:

```bash
docker logs kafka
docker logs clickhouse
```

## Arret Et Nettoyage

Arreter les services:

```bash
docker compose -f src/main/resources/kafka-docker-compose.yml down
```

Ou manuellement:

```bash
sudo docker stop zookeeper kafka clickhouse
sudo docker rm zookeeper kafka clickhouse
```

## Diagramme

Schema global disponible ici:

`src/main/resources/img/flow_diagram.jpg`

## Depannage Rapide

- Verifier Java/Maven:

```bash
java -version
mvn -v
```

- Verifier containers:

```bash
docker ps
```

- Verifier port API:

```bash
curl http://localhost:8080/api/settings
```

- Si un topic Kafka pose probleme, relancer `./start.sh` pour reset propre.

## Institutions contributrices
- École de technologie supérieure (ÉTS)
- École supérieure en informatique 08 Mai 1945 de Sidi Bel Abbès (ESI-SBA)

## Benevoles
A venir.

## Licence

A definir.
