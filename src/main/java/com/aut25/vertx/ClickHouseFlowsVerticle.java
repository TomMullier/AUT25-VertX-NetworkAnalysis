package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.IpPacket;
import org.pcap4j.packet.Packet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickHouseFlowsVerticle extends AbstractVerticle {

        private Connection clickhouseConn;
        private Consumer<String, String> consumer;
        private long delay = 500; // Délai entre les polls Kafka en ms

        @Override
        public void start() throws Exception {
                // Logger setup
                Logger logger = LoggerFactory.getLogger(ClickHouseFlowsVerticle.class);
                logger.info(Colors.GREEN + "[ CLICKHOUSE FLOWS INGESTION VERTICLE ] Starting ClickHouseFlowsVerticle..."
                                + Colors.RESET);

                // Connect to ClickHouse
                String jdbcUrl = "jdbc:clickhouse://localhost:8123/network_analysis";
                clickhouseConn = DriverManager.getConnection(jdbcUrl, "admin", "admin");

                // Kafka Consumer setup
                Properties props = new Properties();
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "network-analysis-group");
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singletonList("network-flows"));

                // Poll Kafka and insert into ClickHouse periodically
                vertx.setPeriodic(delay, id -> {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                        for (ConsumerRecord<String, String> record : records) {
                                if (record.value() == null || record.value().isEmpty()
                                                || record.value().equals("reset")) {
                                        continue; // Skip empty records
                                }
                                try {
                                        JsonObject json = new JsonObject(record.value());

                                        // Extract fields
                                        // jo.put("firstSeen", f.firstSeen);
                                        // jo.put("lastSeen", f.lastSeen);
                                        // jo.put("srcIp", f.srcIp);
                                        // jo.put("dstIp", f.dstIp);
                                        // jo.put("srcPort", f.srcPort);
                                        // jo.put("dstPort", f.dstPort);
                                        // jo.put("protocol", f.protocol);
                                        // jo.put("bytes", f.bytes);
                                        // jo.put("packetCount", f.packetCount);
                                        // jo.put("durationMs", f.lastSeen - f.firstSeen);
                                        // jo.put("flowKey", f.key);
                                        try {
                                                // In this code snippet, the `String` class is being
                                                // used as a parameter for setting the key and value
                                                // deserializers in the Kafka consumer setup.
                                                // Specifically, it is used in the following lines:
                                                String flowId = UUID.randomUUID().toString();
                                                long firstSeen = json.getLong("firstSeen", System.currentTimeMillis());
                                                long lastSeen = json.getLong("lastSeen", firstSeen);
                                                String srcIp = json.getString("srcIp", "0.0.0.0");
                                                String dstIp = json.getString("dstIp", "0.0.0.0");

                                                String srcPort = json.getString("srcPort", "0");
                                                String dstPort = json.getString("dstPort", "0");

                                                String protocol = json.getString("protocol", "UNKNOWN");
                                                long bytes = json.getLong("bytes", 0L);
                                                long packetCount = json.getLong("packetCount", 0L);
                                                long durationMs = json.getLong("durationMs", lastSeen - firstSeen);
                                                String flowKey = json.getString("flowKey", "N/A");

                                                logger.debug(Colors.YELLOW
                                                                + "[ CLICKHOUSE FLOWS INGESTION VERTICLE ] Packet to insert: "
                                                                + json.encodePrettily() + Colors.RESET);

                                                // Insert into ClickHouse
                                                String insertSQL = "INSERT INTO network_flows (id, firstSeen, lastSeen, srcIp, dstIp, srcPort, dstPort, protocol, bytes, packets, durationMs, flowKey) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                                                try (PreparedStatement pstmt = clickhouseConn
                                                                .prepareStatement(insertSQL)) {
                                                        pstmt.setString(1, flowId);
                                                        pstmt.setLong(2, firstSeen);
                                                        pstmt.setLong(3, lastSeen);
                                                        pstmt.setString(4, srcIp);
                                                        pstmt.setString(5, dstIp);
                                                        pstmt.setString(6, srcPort);
                                                        pstmt.setString(7, dstPort);
                                                        pstmt.setString(8, protocol);
                                                        pstmt.setLong(9, bytes);
                                                        pstmt.setLong(10, packetCount);
                                                        pstmt.setLong(11, durationMs);
                                                        pstmt.setString(12, flowKey);
                                                        pstmt.executeUpdate();
                                                } catch (Exception e) {
                                                        logger.error(Colors.RED
                                                                        + "[ CLICKHOUSE FLOWS INGESTION VERTICLE ] Error inserting into ClickHouse: "
                                                                        + e.getMessage() + Colors.RESET);
                                                }

                                                logger.debug(Colors.YELLOW
                                                                + "[ CLICKHOUSE FLOWS INGESTION VERTICLE ] Packet inserted into ClickHouse"
                                                                + Colors.RESET);
                                        } catch (Exception e) {
                                                logger.error(Colors.RED
                                                                + "[ CLICKHOUSE FLOWS INGESTION VERTICLE ] Error parsing JSON fields: "
                                                                + e.getMessage() + Colors.RESET);
                                                logger.info(Colors.RED
                                                                + "[ CLICKHOUSE FLOWS INGESTION VERTICLE ] Skipping record: "
                                                                + json.encodePrettily() + Colors.RESET);
                                                // Display type of every field in json for debug
                                                json.fieldNames().forEach(field -> {
                                                        Object value = json.getValue(field);
                                                        logger.info(Colors.CYAN + "[ DEBUG ] Field: " + field
                                                                        + ", Type: "
                                                                        + (value != null ? value.getClass()
                                                                                        .getSimpleName() : "null")
                                                                        + Colors.RESET);
                                                });

                                                continue; // Skip this record if parsing fails
                                        }

                                } catch (Exception e) {
                                        logger.error(Colors.RED
                                                        + "[ CLICKHOUSE FLOWS INGESTION VERTICLE ] Error processing record: "
                                                        + e.getMessage() + Colors.RESET);
                                }
                        }
                        consumer.commitSync();
                });

                logger.info(Colors.GREEN
                                + "[ CLICKHOUSE FLOWS INGESTION VERTICLE ] ClickHousePacketVerticle deployed successfully!"
                                + Colors.RESET);

        }

        @Override
        public void stop() throws Exception {
                if (consumer != null)
                        consumer.close();
                if (clickhouseConn != null)
                        clickhouseConn.close();
                Logger logger = LoggerFactory.getLogger(ClickHousePacketVerticle.class);
                logger.info(Colors.RED + "[ CLICKHOUSE FLOWS INGESTION VERTICLE ] ClickHousePacketVerticle stopped."
                                + Colors.RESET);
                super.stop();
        }
}
