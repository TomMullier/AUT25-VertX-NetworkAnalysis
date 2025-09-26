package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.IpPacket;
import org.pcap4j.packet.Packet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickHouseFlowsVerticle extends AbstractVerticle {

        private Connection clickhouseConn;
        private KafkaConsumer<String, String> consumer;
        private long delay = 500; // Délai entre les polls Kafka en ms

        private final AtomicBoolean running = new AtomicBoolean(true);

        @Override
        public void start() throws Exception {
                Logger logger = LoggerFactory.getLogger(ClickHouseFlowsVerticle.class);
                logger.info("[ CLICKHOUSE FLOWS VERTICLE ] Starting ClickHouseFlowsVerticle...");

                // Connect to ClickHouse
                String jdbcUrl = "jdbc:clickhouse://localhost:8123/network_analysis";
                clickhouseConn = DriverManager.getConnection(jdbcUrl, "admin", "admin");

                // KafkaConsumer Vert.x configuration
                Map<String, String> config = new HashMap<>();
                config.put("bootstrap.servers", "localhost:9092");
                config.put("group.id", "network-analysis-group");
                config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                config.put("auto.offset.reset", "earliest");
                config.put("enable.auto.commit", "false"); // commit manuel

                consumer = KafkaConsumer.create(vertx, config);

                consumer.handler(record -> {
                        if (!running.get())
                                return;

                        String value = record.value();
                        if (value == null || value.isEmpty() || value.equals("reset")) {
                                return;
                        }

                        try {
                                JsonObject json = new JsonObject(value);

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

                                // Insert into ClickHouse
                                String insertSQL = "INSERT INTO network_flows " +
                                                "(id, firstSeen, lastSeen, srcIp, dstIp, srcPort, dstPort, protocol, bytes, packets, durationMs, flowKey) "
                                                +
                                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                                try (PreparedStatement pstmt = clickhouseConn.prepareStatement(insertSQL)) {
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
                                }

                        } catch (Exception e) {
                                logger.error("[ CLICKHOUSE FLOWS VERTICLE ] Error processing record: {}",
                                                e.getMessage());
                        }

                        // Commit manuel après traitement
                        consumer.commit(ar -> {
                                if (ar.failed()) {

                                        if (ar.cause().getMessage() != null) {
                                                logger.error(
                                                                "[ CLICKHOUSE FLOWS VERTICLE ] Commit failed: "
                                                                                + ar.cause().getMessage());
                                        }

                                }
                        });
                });

                // Subscribe to topic
                consumer.subscribe("network-flows", ar -> {
                        if (ar.succeeded()) {
                                logger.info("[ CLICKHOUSE FLOWS VERTICLE ] Subscribed to topic network-flows");
                        } else {
                                logger.error("[ CLICKHOUSE FLOWS VERTICLE ] Failed to subscribe: {}",
                                                ar.cause().getMessage());
                        }
                });

                logger.info("[ CLICKHOUSE FLOWS VERTICLE ] ClickHouseFlowsVerticle deployed successfully!");
        }

        @Override
        public void stop() throws Exception {
                running.set(false);
                if (consumer != null)
                        consumer.close();
                if (clickhouseConn != null)
                        clickhouseConn.close();
                Logger logger = LoggerFactory.getLogger(ClickHouseFlowsVerticle.class);
                logger.info(Colors.RED + "[ CLICKHOUSE FLOWS VERTICLE ] ClickHouseFlowsVerticle stopped."
                                + Colors.RESET);
        }
}
