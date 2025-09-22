package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Iterator;
import java.io.EOFException;
import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.pcap4j.core.PacketListener;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.PcapNetworkInterface;
import org.pcap4j.core.Pcaps;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.namednumber.DataLinkType;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.concurrent.Executors;

public class IngestionVerticle extends AbstractVerticle {

        private static final Logger logger = LoggerFactory.getLogger(IngestionVerticle.class);
        private KafkaProducer<String, String> producer;

        @Override
        public void start() throws Exception {
                logger.info(Colors.GREEN + "[ INGESTION VERTICLE ] Starting IngestionVerticle..." + Colors.RESET);

                // Config file : get debug mode
                JsonObject config = new JsonObject(
                                new String(Files.readAllBytes(Paths.get("src/main/resources/config.json"))));

                /*
                 * Get mode from config file.
                 * Can be :
                 * - json (default for debug)
                 * - pcap
                 * - realtime
                 */
                String mode = config.getString("mode", "json");

                /* ----------------------- Creation of Kafka producer ----------------------- */
                configureKafkaProducer();
                producer.send(new ProducerRecord<>("network-data", "reset"), (metadata, exception) -> {
                        if (exception != null) {
                                logger.error("[ INGESTION VERTICLE ] Failed to reset Kafka topic: "
                                                + exception.getMessage());
                        } else {
                                logger.info("[ INGESTION VERTICLE ] Kafka topic 'network-data' reset successfully.");
                        }
                });

                logger.info("[ INGESTION VERTICLE ][ CONFIG ] Mode: " + mode.toUpperCase());
                switch (mode) {
                        case "json":
                                /* ------------------------ Parameters for json mode ------------------------ */
                                JsonObject fileConfig = config.getJsonObject("json", new JsonObject());
                                // Get path and ingestion interval from config file
                                String filePath = fileConfig.getString("file-path", "data/sample_data.json");
                                int interval = fileConfig.getInteger("ingestion-interval-ms", 1000);
                                logger.info("[ INGESTION VERTICLE ][ CONFIG ] File path: " + filePath);
                                logger.info("[ INGESTION VERTICLE ][ CONFIG ] Ingestion interval (ms): "
                                                + interval);

                                /* ------------------------ READ FILE AND PARSE JSON ------------------------ */
                                List<JsonObject> records;
                                try {
                                        String content = Files.readString(Paths.get(filePath), StandardCharsets.UTF_8);
                                        ObjectMapper mapper = new ObjectMapper();

                                        List<Map<String, Object>> listOfMaps = mapper.readValue(
                                                        content, new TypeReference<List<Map<String, Object>>>() {
                                                        });

                                        records = listOfMaps.stream()
                                                        .map(JsonObject::mapFrom)
                                                        .toList();

                                } catch (Exception e) {
                                        logger.error("[ INGESTION VERTICLE ] Failed to read or parse file: "
                                                        + e.getMessage());
                                        return;
                                }

                                /* --------------- Publication of records at regular intervals -------------- */
                                // AtomicReference to keep track of the iterator state
                                AtomicReference<Iterator<JsonObject>> iteratorRef = new AtomicReference<>(
                                                records.iterator());

                                vertx.setPeriodic(interval, id -> {
                                        Iterator<JsonObject> it = iteratorRef.get();
                                        if (!it.hasNext()) {
                                                // Reset iterator if end of list is reached to loop
                                                it = records.iterator();
                                                iteratorRef.set(it);
                                                logger.info(
                                                                "[ INGESTION VERTICLE ] End of file reached. Looping again...");
                                        }
                                        // Publication of the next record
                                        JsonObject record = it.next();
                                        // Publish record on Kafka topic "network-data"
                                        ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(
                                                        "network-data", record.encode());
                                        producer.send(kafkaRecord, (metadata, exception) -> {
                                                if (exception != null) {
                                                        logger.error(
                                                                        "[ INGESTION VERTICLE ] Failed to send record to Kafka: "
                                                                                        + exception.getMessage());
                                                } else {
                                                        logger.debug(
                                                                        "[ INGESTION VERTICLE ] Record sent to Kafka topic "
                                                                                        + metadata.topic()
                                                                                        + " partition "
                                                                                        + metadata.partition()
                                                                                        + " offset "
                                                                                        + metadata.offset());
                                                }
                                        });
                                        logger.debug("[ INGESTION VERTICLE ] Published record from file: \n"
                                                        + record.encodePrettily());
                                });

                                break;

                        case "pcap":
                                // TODO : implement pcap replay for ingestion
                                JsonObject pcapConfig = config.getJsonObject("pcap", new JsonObject());
                                String pcapFilePath = pcapConfig.getString("file-path",
                                                "src/main/resources/datapcap-sample.pcap");
                                logger.debug("[ INGESTION VERTICLE ][ CONFIG ] PCAP File path: " + pcapFilePath);

                                ingestFromPcap(pcapFilePath);

                                break;

                        case "realtime":
                                JsonObject rtConfig = config.getJsonObject("realtime", new JsonObject());
                                String networkInterface = rtConfig.getString("interface", "eth0");
                                logger.debug("[ INGESTION VERTICLE ][ CONFIG ] Network Interface: "
                                                + networkInterface);
                                ingestInRealTime(networkInterface);
                                break;
                        default:
                                logger.error(
                                                "[ INGESTION VERTICLE ] Unknown mode. No ingestion will be performed.");
                                break;
                }

                logger.info("[ INGESTION VERTICLE ] IngestionVerticle started!");

        }

        /* ------------------- Configuration of the Kafka producer ------------------ */
        /**
         * Configure Kafka producer with necessary properties
         */
        private void configureKafkaProducer() {
                // Config Kafka Producer
                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // ton broker Kafka
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

                producer = new KafkaProducer<>(props);

                logger.info("[ INGESTION VERTICLE ] Kafka Producer initialized.");

                logger.debug("[ INGESTION VERTICLE ] Kafka producer configured with bootstrap servers: "
                                + props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        }

        /* ----------------------------- Mode Realtime ------------------------------ */
        static class Task implements Runnable {
                private final PcapHandle handle;
                private final PacketListener listener;

                Task(PcapHandle handle, PacketListener listener) {
                        this.handle = handle;
                        this.listener = listener;
                }

                @Override
                public void run() {
                        try {
                                handle.loop(-1, listener); // -1 = capture infinie
                        } catch (Exception e) {
                                e.printStackTrace();
                        } finally {
                                try {
                                        handle.close();
                                } catch (Exception ignored) {
                                }
                        }
                }
        }

        /**
         * Ingest packets in real-time (e.g., from a network interface) and publish them
         * to Kafka.
         */
        private void ingestInRealTime(String networkInterface) {
                PcapNetworkInterface nif;
                if (networkInterface == null) {
                        logger.error("[ INGESTION VERTICLE ] No network interface specified.");
                        return;
                }
                try {
                        nif = Pcaps.getDevByName(networkInterface);
                        if (nif == null) {
                                logger.error("[ INGESTION VERTICLE ] Network interface not found: " + networkInterface);
                                return;
                        }

                        logger.info("[ INGESTION VERTICLE ] Capturing packets from network interface: "
                                        + networkInterface);
                        final int snapLen = 65536; // Capture all packets, no truncation
                        final int timeout = 10; // 10 ms
                        final PcapHandle handle = nif.openLive(snapLen,
                                        PcapNetworkInterface.PromiscuousMode.PROMISCUOUS, timeout);
                        AtomicReference<Long> timestamp = new AtomicReference<>(System.currentTimeMillis());
                        PacketListener listener = packet -> {
                                long packetTimestamp = System.currentTimeMillis();
                                long delay = packetTimestamp - timestamp.get();
                                timestamp.set(packetTimestamp);
                                processPacket(packet, delay);
                        };

                        ExecutorService pool = Executors.newSingleThreadExecutor();
                        Task t = new Task(handle, listener);
                        pool.execute(t);

                } catch (PcapNativeException e) {
                        logger.error("[ INGESTION VERTICLE ] Error accessing network interface: " + e.getMessage());
                        logger.error("[ INGESTION VERTICLE ] Please check if the interface name is correct and if the necessary permissions are granted.");
                        return;
                } catch (Exception e) {
                        logger.error("[ INGESTION VERTICLE ] Unexpected error: " + e.getMessage());
                        return;
                } finally {
                        logger.info("[ INGESTION VERTICLE ] Exiting ingestInRealTime method.");
                }

        }

        /* -------------------------------- Mode pcap ------------------------------- */
        /**
         * Ingest packets from a pcap file and publish them to Kafka, maintaining the
         * original
         * timing between packets.
         * 
         * @param pcapFilePath Path to the pcap file
         */
        private void ingestFromPcap(String pcapFilePath) {
                PcapHandle handle;
                try {
                        handle = Pcaps.openOffline(pcapFilePath, PcapHandle.TimestampPrecision.NANO);
                } catch (PcapNativeException e) {
                        logger.error("[ INGESTION VERTICLE ] Failed to open pcap file: " + e.getMessage());
                        return;
                }
                logger.info("[ INGESTION VERTICLE ] PCAP file opened successfully: {}", pcapFilePath);
                DataLinkType dlt = handle.getDlt();
                logger.debug("[ INGESTION VERTICLE ] Data Link Type: {}", dlt);

                try {
                        Packet firstPacket = handle.getNextPacketEx();
                        if (firstPacket == null) {
                                logger.warn("[ INGESTION VERTICLE ] No packets found in file {}", pcapFilePath);
                                return;
                        }

                        // Reference timestamp and time tracking
                        Timestamp firstTimestamp = handle.getTimestamp();
                        long startTime = System.nanoTime();

                        logger.info("[ INGESTION VERTICLE ] Starting replay of packets...");

                        // Process the first packet
                        processPacket(firstPacket, 0);

                        // Process remaining packets
                        while (true) {
                                try {
                                        Packet packet = handle.getNextPacketEx();
                                        if (packet == null)
                                                break;

                                        Timestamp currentTs = handle.getTimestamp();

                                        // Calculate delay relative to the first packet
                                        long expectedDelta = (currentTs.getTime() - firstTimestamp.getTime());
                                        long realDelta = (System.nanoTime() - startTime) / 1_000_000; // in ms
                                        long delay = expectedDelta - realDelta;
                                        if (expectedDelta > realDelta) {
                                                Thread.sleep(delay); // wait to maintain original pace

                                        }

                                        processPacket(packet, delay);

                                } catch (EOFException e) {
                                        logger.info("[ INGESTION VERTICLE ] End of PCAP file reached.");
                                        break;
                                }
                        }

                } catch (Exception e) {
                        logger.error("[ INGESTION VERTICLE ] Error reading packets: {}", e.getMessage());
                } finally {
                        handle.close();
                        logger.info("[ INGESTION VERTICLE ] PCAP handle closed.");
                }
        }

        /**
         * Process and send a packet to Kafka
         *
         * @param packet The packet to process
         */
        private void processPacket(Packet packet, long delay) {
                if (packet == null)
                        return;
                byte[] rawData = packet.getRawData();
                // Convert raw data to hex string for JSON representation
                String base64Packet = Base64.getEncoder().encodeToString(rawData);
                // Construire l'objet JSON enrichi
                JsonObject record = new JsonObject()
                                .put("timestamp", System.currentTimeMillis())
                                .put("delay", delay)
                                .put("rawPacket", base64Packet);

                logger.debug("[ INGESTION VERTICLE ] Processed packet: \n{}", record.encodePrettily());

                // Publish record on Kafka topic "network-data"
                ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(
                                "network-data", record.encode());
                producer.send(kafkaRecord, (metadata, exception) -> {
                        if (exception != null) {
                                logger.error(
                                                "[ INGESTION VERTICLE ] Failed to send packet record to Kafka: "
                                                                + exception.getMessage());
                        } else {
                                logger.debug(
                                                "[ INGESTION VERTICLE ] Packet record sent to Kafka topic "
                                                                + metadata.topic()
                                                                + " partition "
                                                                + metadata.partition()
                                                                + " offset "
                                                                + metadata.offset()
                                                                + (delay > 0 ? " (delayed by " + delay + " ms)" : ""));
                        }
                });
                logger.debug("[ INGESTION VERTICLE ] Published packet: \n{}", record.encodePrettily());
        }

        /* -------------------------------------------------------------------------- */
        /**
         * Cleanup resources on verticle stop
         */
        @Override
        public void stop() {
                if (producer != null) {
                        producer.close();
                        logger.info("[ INGESTION VERTICLE ] Kafka Producer closed.");
                }
                logger.info(Colors.RED + "[ INGESTION VERTICLE ] IngestionVerticle stopped!" + Colors.RESET);
        }
}