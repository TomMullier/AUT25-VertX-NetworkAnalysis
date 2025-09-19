package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.consumer.*;
import org.pcap4j.packet.IpPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.Properties;
import static java.lang.Thread.sleep;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.pcap4j.packet.Packet;
import java.util.Base64;
import org.pcap4j.packet.EthernetPacket;

public class AnalyseVerticle extends AbstractVerticle {

        private static final Logger logger = LoggerFactory.getLogger(AnalyseVerticle.class);
        KafkaConsumer<String, String> consumer;
        String mode;

        @Override
        public void start() throws Exception {
                logger.info(Colors.GREEN + "[ ANALYSE VERTICLE ] Starting AnalyseVerticle..." + Colors.RESET);
                // Get mode from config, default to "pcap"
                JsonObject config = new JsonObject(
                                new String(Files.readAllBytes(Paths.get("src/main/resources/config.json"))));
                mode = config.getString("mode", "pcap").toLowerCase();

                // Setup Kafka consumer
                Properties props = new Properties();
                props.setProperty("bootstrap.servers", "localhost:9092");
                props.setProperty("group.id", "analyse-group");
                props.setProperty("enable.auto.commit", "false");
                props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                props.setProperty("auto.offset.reset", "earliest");
                consumer = new KafkaConsumer<>(props);

                // Start reading from Kafka topic every 2 seconds if mode is pcap
                switch (mode) {
                        case "pcap":
                                readFromKafkaActionEveryDelay_PCAP("network-data", 2000);
                                break;
                        case "json":
                                readFromKafkaActionEveryDelay_JSON("network-data", 2000);
                                break;
                        case "realtime":
                                logger.info(Colors.GREEN
                                                + "[ ANALYSE VERTICLE ] Mode set to REALTIME. No action defined yet."
                                                + Colors.RESET);
                                break;

                        default:
                                logger.warn(Colors.YELLOW
                                                + "[ ANALYSE VERTICLE ] Unknown mode '{}', defaulting to PCAP."
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
        private void readFromKafkaActionEveryDelay_PCAP(String topic, long delay) {
                logger.info(Colors.CYAN + "[ ANALYSE VERTICLE ] Subscribing to Kafka topic: " + topic + Colors.RESET);

                // Subscribe to the topic
                consumer.subscribe(Arrays.asList(topic));

                // Use executeBlocking to avoid blocking the event loop
                // Read messages in a loop with the specified delay
                vertx.executeBlocking(promise -> {
                        try {
                                while (true) {
                                        ConsumerRecords<String, String> records = consumer
                                                        .poll(java.time.Duration.ofMillis(100));
                                        for (ConsumerRecord<String, String> record : records) {
                                                try {
                                                        JsonObject json = new JsonObject(record.value());
                                                        try {
                                                                String srcIp = "";
                                                                String dstIp = "";
                                                                String protocol = "UNKNOWN";

                                                                // Parse the raw packet data
                                                                String rawPacketBase64 = json.getString("rawPacket");
                                                                if (rawPacketBase64 == null) {
                                                                        logger.warn("[ ANALYSE VERTICLE ] No rawPacket field in JSON.");
                                                                        continue;
                                                                }
                                                                byte[] rawData = Base64.getDecoder()
                                                                                .decode(rawPacketBase64);
                                                                Packet packet = EthernetPacket.newPacket(rawData, 0,
                                                                                rawData.length);
                                                                // Vérifie si c’est un paquet IP
                                                                if (packet.contains(IpPacket.class)) {
                                                                        IpPacket ipPacket = packet.get(IpPacket.class);
                                                                        srcIp = ipPacket.getHeader().getSrcAddr()
                                                                                        .getHostAddress();
                                                                        dstIp = ipPacket.getHeader().getDstAddr()
                                                                                        .getHostAddress();
                                                                        if (ipPacket.getHeader()
                                                                                        .getProtocol() != null) {
                                                                                protocol = ipPacket.getHeader()
                                                                                                .getProtocol().name();
                                                                        } else {
                                                                                protocol = "UNKNOWN";
                                                                        }
                                                                }
                                                                // Add parsed fields to JSON
                                                                json.put("srcIp", srcIp);
                                                                json.put("dstIp", dstIp);
                                                                json.put("protocol", protocol);
                                                                json.put("bytes", packet.length());
                                                                // Decode base64 to raw packet string
                                                                json.put("rawPacket", packet.toString());
                                                        } catch (Exception e) {
                                                                logger.warn("[ INGESTION VERTICLE ] Could not parse IP/transport layer: {}",
                                                                                e.getMessage());
                                                        }

                                                        // Log the parsed data
                                                        logger.info(Colors.CYAN
                                                                        + "[ ANALYSE VERTICLE ] Received record: "
                                                                        + json.encodePrettily() + Colors.RESET);
                                                } catch (Exception e) {
                                                        logger.error(Colors.RED
                                                                        + "[ ANALYSE VERTICLE ] Failed to parse JSON: "
                                                                        + e.getMessage() + Colors.RESET);
                                                        logger.error(Colors.RED
                                                                        + "[ ANALYSE VERTICLE ] Original record: "
                                                                        + record.value() + Colors.RESET);
                                                }
                                                // TODO Process the JSON data as needed

                                                // Wait before printing the next record
                                                sleep(delay);

                                        }
                                        if (!records.isEmpty()) {
                                                consumer.commitSync();
                                                logger.debug("[ ANALYSE VERTICLE ] Offsets committed.");
                                        }
                                }
                        } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                logger.error(Colors.RED + "[ ANALYSE VERTICLE ] Consumer interrupted: " + e.getMessage()
                                                + Colors.RESET);
                        } catch (Exception e) {
                                logger.error("[ ANALYSE VERTICLE ] Error in consumer loop: "
                                                + e.getMessage());
                        } finally {
                                consumer.close();
                                logger.info("[ ANALYSE VERTICLE ] Kafka consumer closed.");
                        }
                }, res -> {
                        if (res.succeeded()) {
                                logger.info("[ ANALYSE VERTICLE ] Finished processing Kafka messages.");
                        } else {
                                logger.error("[ ANALYSE VERTICLE ] Failed to process Kafka messages: "
                                                + res.cause());
                        }
                });

                logger.info("[ ANALYSE VERTICLE ] Kafka consumer setup complete.");

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        consumer.close();
                        logger.info("[ ANALYSE VERTICLE ] Kafka consumer closed.");
                }));

        }

        private void readFromKafkaActionEveryDelay_JSON(String topic, long delay) {
                logger.info(Colors.CYAN + "[ ANALYSE VERTICLE ] Subscribing to Kafka topic: " + topic + Colors.RESET);

                // Subscribe to the topic
                consumer.subscribe(Arrays.asList(topic));

                // Use executeBlocking to avoid blocking the event loop
                // Read messages in a loop with the specified delay
                vertx.executeBlocking(promise -> {
                        try {
                                while (true) {
                                        ConsumerRecords<String, String> records = consumer
                                                        .poll(java.time.Duration.ofMillis(100));
                                        for (ConsumerRecord<String, String> record : records) {
                                                try {
                                                        JsonObject json = new JsonObject(record.value());

                                                        // Log the received JSON data
                                                        logger.info(Colors.CYAN
                                                                        + "[ ANALYSE VERTICLE ] Received record: "
                                                                        + json.encodePrettily() + Colors.RESET);
                                                } catch (Exception e) {
                                                        logger.error(Colors.RED
                                                                        + "[ ANALYSE VERTICLE ] Failed to parse JSON: "
                                                                        + e.getMessage() + Colors.RESET);
                                                        logger.error(Colors.RED
                                                                        + "[ ANALYSE VERTICLE ] Original record: "
                                                                        + record.value() + Colors.RESET);
                                                }
                                                // TODO Process the JSON data as needed

                                                // Wait before printing the next record
                                                sleep(delay);

                                        }
                                        if (!records.isEmpty()) {
                                                consumer.commitSync();
                                                logger.debug("[ ANALYSE VERTICLE ] Offsets committed.");
                                        }
                                }
                        } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                logger.error(Colors.RED + "[ ANALYSE VERTICLE ] Consumer interrupted: " + e.getMessage()
                                                + Colors.RESET);
                        } catch (Exception e) {
                                logger.error("[ ANALYSE VERTICLE ] Error in consumer loop: "
                                                + e.getMessage());
                        } finally {
                                consumer.close();
                                logger.info("[ ANALYSE VERTICLE ] Kafka consumer closed.");
                        }
                }, res -> {
                        if (res.succeeded()) {
                                logger.info("[ ANALYSE VERTICLE ] Finished processing Kafka messages.");
                        } else {
                                logger.error("[ ANALYSE VERTICLE ] Failed to process Kafka messages: "
                                                + res.cause());
                        }
                });

                logger.info("[ ANALYSE VERTICLE ] Kafka consumer setup complete.");
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        consumer.close();
                        logger.info("[ ANALYSE VERTICLE ] Kafka consumer closed.");
                }));
        }

        @Override
        public void stop() {
                logger.info(Colors.RED + "[ ANALYSE VERTICLE ] AnalyseVerticle stopped!" + Colors.RESET);
        }

}
