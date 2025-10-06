package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.consumer.*;
import org.pcap4j.packet.IpPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.pcap4j.packet.Packet;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.pcap4j.packet.EthernetPacket;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

public class AnalyseVerticle extends AbstractVerticle {

        private static final Logger logger = LoggerFactory.getLogger(AnalyseVerticle.class);
        KafkaConsumer<String, String> consumer;
        String mode;
        private final AtomicBoolean running = new AtomicBoolean(true);// Analyse avec nDPI

        @Override
        public void start() throws Exception {
                logger.info(Colors.GREEN + "[ ANALYSE VERTICLE ]              Starting AnalyseVerticle..."
                                + Colors.RESET);
                // Get mode from config, default to "pcap"
                JsonObject config = new JsonObject(
                                new String(Files.readAllBytes(Paths.get("src/main/resources/config.json"))));
                mode = config.getString("mode", "pcap").toLowerCase();

                
                // Start reading from Kafka topic every 2 seconds if mode is pcap
                switch (mode) {
                        case "pcap":
                                readFromKafka_ActionEveryDelay("network-data", 2000);
                                break;
                        case "json":
                                readFromKafka_ActionEveryDelay("network-data", 2000);
                                break;
                        case "realtime":
                                readFromKafka_ActionEveryDelay("network-data", 2000);
                                break;
                        case "none":
                                logger.warn(Colors.YELLOW
                                                + "[ ANALYSE VERTICLE ]              Mode is set to 'none', no analysis will be performed."
                                                + Colors.RESET);
                                return;
                        default:
                                logger.warn(Colors.YELLOW
                                                + "[ ANALYSE VERTICLE ]              Unknown mode '{}', defaulting to PCAP."
                                                + Colors.RESET,
                                                mode);
                                mode = "pcap";
                }
        }

        /**
         * Reads messages from a Kafka topic at regular intervals if mode is "pcap".
         * 
         * @param topic the Kafka topic to subscribe to
         * @param delay the delay in milliseconds between reads
         */
        private void readFromKafka_ActionEveryDelay(String topic, long delay) {
                logger.info(Colors.CYAN + "[ ANALYSE VERTICLE ]              Subscribing to Kafka topic: " + topic
                                + Colors.RESET);

                // Config Kafka
                Map<String, String> config = new HashMap<>();
                config.put("bootstrap.servers", "localhost:9092");
                config.put("group.id", "analyse-group");
                config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                config.put("auto.offset.reset", "earliest");
                config.put("enable.auto.commit", "false");

                // Équivalents des propriétés avancées
                config.put("max.poll.interval.ms", "600000"); // 10 minutes
                config.put("session.timeout.ms", "30000"); // 30s
                config.put("heartbeat.interval.ms", "10000"); // 10s

                KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);

                consumer.subscribe(topic)
                                .onSuccess(v -> logger.info(
                                                Colors.CYAN + "[ ANALYSE VERTICLE ]              Subscribed to " + topic
                                                                + Colors.RESET))
                                .onFailure(err -> logger.error(
                                                "[ ANALYSE VERTICLE ]              Failed to subscribe: "
                                                                + err.getMessage()));

                consumer.handler(record -> {
                        vertx.setTimer(delay, tid -> {
                                try {
                                        String value = record.value();
                                        if (value == null || value.isEmpty() || value.equals("reset")) {
                                                logger.warn("[ ANALYSE VERTICLE ]              Received empty record, skipping.");
                                                return;
                                        }

                                        JsonObject json = new JsonObject(value);

                                        try {
                                                String srcIp = "";
                                                String dstIp = "";
                                                String protocol = "UNKNOWN";

                                                // Parse le paquet brut
                                                String rawPacketBase64 = json.getString("rawPacket");
                                                if (rawPacketBase64 == null) {
                                                        logger.warn("[ ANALYSE VERTICLE ]              No rawPacket field in JSON.");
                                                        return;
                                                }
                                                byte[] rawData = Base64.getDecoder().decode(rawPacketBase64);
                                                Packet packet = EthernetPacket.newPacket(rawData, 0, rawData.length);

                                                if (packet.contains(IpPacket.class)) {
                                                        IpPacket ipPacket = packet.get(IpPacket.class);
                                                        srcIp = ipPacket.getHeader().getSrcAddr().getHostAddress();
                                                        dstIp = ipPacket.getHeader().getDstAddr().getHostAddress();
                                                        if (ipPacket.getHeader().getProtocol() != null) {
                                                                protocol = ipPacket.getHeader().getProtocol().name();
                                                        }
                                                }

                                                json.put("srcIp", srcIp);
                                                json.put("dstIp", dstIp);
                                                json.put("protocol", protocol);
                                                json.put("bytes", packet.length());
                                                String ltab = "\t";
                                                json.put("rawPacket",
                                                                ltab.concat(packet.toString().replace("\n", "\n\t\t")));

                                        } catch (Exception e) {
                                                logger.warn("[ INGESTION VERTICLE ] Could not parse IP/transport layer: {}",
                                                                e.getMessage());
                                        }

                                        // Log le paquet
                                        logger.debug(Colors.CYAN + "[ ANALYSE VERTICLE ]              Parsed JSON: " +
                                                        json.encodePrettily() + Colors.RESET);

                                        logger.debug(Colors.YELLOW
                                                        + "[ ANALYSE VERTICLE ]              Raw Packet Data: "
                                                        + Colors.RESET);
                                        logger.debug(Colors.YELLOW + json.getValue("rawPacket").toString()
                                                        + Colors.RESET);

                                        // Commit manuel (async)
                                        consumer.commit()
                                                        .onSuccess(v -> logger.debug(
                                                                        "[ ANALYSE VERTICLE ]              Offsets committed."))
                                                        .onFailure(err -> {
                                                                if (err.getMessage() != null) {
                                                                        logger.error(
                                                                                        "[ ANALYSE VERTICLE ]              Commit failed: "
                                                                                                        + err.getMessage());
                                                                }
                                                        });

                                } catch (Exception e) {
                                        logger.error(Colors.RED
                                                        + "[ ANALYSE VERTICLE ]              Failed to parse JSON: "
                                                        + e.getMessage() + Colors.RESET);
                                        logger.error(Colors.RED + "[ ANALYSE VERTICLE ]              Original record: "
                                                        + record.value() + Colors.RESET);
                                }
                        });
                });

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        consumer.close()
                                        .onSuccess(v -> logger
                                                        .info("[ ANALYSE VERTICLE ]              Kafka consumer closed."))
                                        .onFailure(err -> logger
                                                        .error("[ ANALYSE VERTICLE ]              Error closing Kafka consumer: "
                                                                        + err.getMessage()));
                }));
        }

        @Override
        public void stop() throws Exception {
                if (consumer != null) {
                        consumer.close()
                                        .onSuccess(v -> logger.info(
                                                        "[ ANALYSE VERTICLE ]              Kafka consumer closed proprement ✅"))
                                        .onFailure(err -> logger.error(
                                                        "[ ANALYSE VERTICLE ]              Error closing consumer",
                                                        err));
                }
                logger.info(Colors.RED + "[ ANALYSE VERTICLE ]              AnalyseVerticle stopped!" + Colors.RESET);
        }

}
