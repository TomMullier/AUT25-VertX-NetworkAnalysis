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

public class ClickHousePacketVerticle extends AbstractVerticle {

        private Connection clickhouseConn;
        private Consumer<String, String> consumer;
        private long delay = 500; // Délai entre les polls Kafka en ms

        @Override
        public void start() throws Exception {
                // Logger setup
                Logger logger = LoggerFactory.getLogger(ClickHousePacketVerticle.class);
                logger.info(Colors.GREEN
                                + "[ CLICKHOUSE PACKET  INGESTION VERTICLE ] Starting ClickHousePacketVerticle..."
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
                consumer.subscribe(Collections.singletonList("network-data"));

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

                                        // Extract fields from JSON
                                        long timestamp = json.getLong("timestamp", System.currentTimeMillis());
                                        String rawPacket = json.getString("rawPacket");
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

                                        int bytes = packet.length();
                                        logger.debug(Colors.YELLOW
                                                        + "[ CLICKHOUSE PACKET  INGESTION VERTICLE ] Packet to insert: "
                                                        + json.encodePrettily() + Colors.RESET);

                                        // Insert into ClickHouse
                                        String sql = "INSERT INTO network_data (id, timestamp, srcIp, dstIp, protocol, bytes, rawPacket) "
                                                        + "VALUES (?, ?, ?, ?, ?, ?, ?)";
                                        try (PreparedStatement pstmt = clickhouseConn.prepareStatement(sql)) {
                                                pstmt.setString(1, UUID.randomUUID().toString());
                                                pstmt.setLong(2, timestamp);
                                                pstmt.setString(3, srcIp);
                                                pstmt.setString(4, dstIp);
                                                pstmt.setString(5, protocol);
                                                pstmt.setInt(6, bytes);
                                                pstmt.setString(7, rawPacket);
                                                pstmt.executeUpdate();

                                                logger.debug(Colors.YELLOW
                                                                + "[ CLICKHOUSE PACKET  INGESTION VERTICLE ] Packet inserted into ClickHouse"
                                                                + Colors.RESET);
                                        }

                                } catch (Exception e) {
                                        logger.error(Colors.RED
                                                        + "[ CLICKHOUSE PACKET  INGESTION VERTICLE ] Error processing record: "
                                                        + e.getMessage() + Colors.RESET);
                                }
                        }
                        consumer.commitSync();
                });

                logger.info(Colors.GREEN
                                + "[ CLICKHOUSE PACKET  INGESTION VERTICLE ] ClickHousePacketVerticle deployed successfully!"
                                + Colors.RESET);
        }

        @Override
        public void stop() throws Exception {
                if (consumer != null)
                        consumer.close();
                if (clickhouseConn != null)
                        clickhouseConn.close();
                Logger logger = LoggerFactory.getLogger(ClickHousePacketVerticle.class);
                logger.info(Colors.RED + "[ CLICKHOUSE PACKET  INGESTION VERTICLE ] ClickHousePacketVerticle stopped."
                                + Colors.RESET);
                super.stop();
        }
}
