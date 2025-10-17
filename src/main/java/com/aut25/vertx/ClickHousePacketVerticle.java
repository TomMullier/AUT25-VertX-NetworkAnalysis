package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.IpPacket;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.TcpPacket;


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

public class ClickHousePacketVerticle extends AbstractVerticle {

        private Connection clickhouseConn;
        private KafkaConsumer<String, String> consumer;
        private final AtomicBoolean running = new AtomicBoolean(true);

        @Override
        public void start() throws Exception {
                Logger logger = LoggerFactory.getLogger(ClickHousePacketVerticle.class);
                logger.info(Colors.GREEN + "[ CLICKHOUSE PACKET VERTICLE ]    Starting ClickHousePacketVerticle..."
                                + Colors.RESET);

                // Connect to ClickHouse
                String jdbcUrl = "jdbc:clickhouse://localhost:8123/network_analysis";
                clickhouseConn = DriverManager.getConnection(jdbcUrl, "admin", "admin");

                // Vert.x KafkaConsumer configuration
                Map<String, String> config = new HashMap<>();
                config.put("bootstrap.servers", "localhost:9092");
                config.put("group.id", "network-analysis-group");
                config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                config.put("auto.offset.reset", "earliest");
                config.put("enable.auto.commit", "false"); // commit manuel

                consumer = KafkaConsumer.create(vertx, config);

                // Handler pour chaque message reçu
                consumer.handler(record -> {
                        if (!running.get())
                                return;

                        String value = record.value();
                        if (value == null || value.isEmpty() || value.equals("reset")) {
                                return; // skip empty/reset records
                        }

                        try {
                                JsonObject json = new JsonObject(value);

                                long timestamp = json.getLong("timestamp", System.currentTimeMillis());
                                String rawPacket = json.getString("rawPacket");
                                String srcIp = "";
                                String dstIp = "";
                                String protocol = "UNKNOWN";

                                // Parse raw packet
                                if (rawPacket != null) {
                                        byte[] rawData = Base64.getDecoder().decode(rawPacket);
                                        Packet packet = EthernetPacket.newPacket(rawData, 0, rawData.length);

                                        if (packet.contains(IpPacket.class)) {
                                                IpPacket ipPacket = packet.get(IpPacket.class);
                                                srcIp = ipPacket.getHeader().getSrcAddr().getHostAddress();
                                                dstIp = ipPacket.getHeader().getDstAddr().getHostAddress();
                                                protocol = ipPacket.getHeader().getProtocol() != null
                                                                ? ipPacket.getHeader().getProtocol().name()
                                                                : "UNKNOWN";
                                        }

                                        // Insert into ClickHouse
                                        String sql = "INSERT INTO network_data (id, timestamp, srcIp, dstIp, protocol, bytes, rawPacket) "
                                                        + "VALUES (?, ?, ?, ?, ?, ?, ?)";
                                        try (PreparedStatement pstmt = clickhouseConn.prepareStatement(sql)) {
                                                pstmt.setString(1, setPacketId(srcIp, dstIp, protocol, timestamp, packet));
                                                pstmt.setLong(2, timestamp);
                                                pstmt.setString(3, srcIp);
                                                pstmt.setString(4, dstIp);
                                                pstmt.setString(5, protocol);
                                                pstmt.setInt(6, packet.length());
                                                pstmt.setString(7, rawPacket);
                                                pstmt.executeUpdate();
                                        }

                                }
                        } catch (Exception e) {
                                logger.error("[ CLICKHOUSE PACKET VERTICLE ]    Error processing record: {}",
                                                e.getMessage());
                        }

                        // Commit manuel après traitement
                        consumer.commit(ar -> {
                                if (ar.failed()) {
                                        if (ar.cause().getMessage() != null) {
                                                logger.error(
                                                                "[ CLICKHOUSE PACKET VERTICLE ]    Commit failed: "
                                                                                + ar.cause().getMessage());
                                        }
                                }
                        });
                });

                // Subscribe au topic
                consumer.subscribe("network-data", ar -> {
                        if (ar.succeeded()) {
                                logger.info(Colors.CYAN
                                                + "[ CLICKHOUSE PACKET VERTICLE ]    Subscribed to topic network-data"
                                                + Colors.RESET);
                        } else {
                                logger.error("[ CLICKHOUSE PACKET VERTICLE ]    Failed to subscribe: {}",
                                                ar.cause().getMessage());
                        }
                });

                logger.debug("[ CLICKHOUSE PACKET VERTICLE ]    ClickHousePacketVerticle deployed successfully!");
        }

        /**
         * Generate a unique packet ID based on source IP, destination IP, protocol, and
         * timestamp
         * 
         * @param sourceIp  IP address of the packet source
         * @param destIp    IP address of the packet destination
         * @param proto     Protocol used in the packet
         * @param timestamp Timestamp of the packet
         * @param packet    The packet object to extract flags
         * @return Unique packet ID
         */
        private String setPacketId(String sourceIp, String destIp, String proto, long timestamp, Packet packet) {
                String flag="";
                if (packet.contains(TcpPacket.class)) {
                                TcpPacket tcpPacket = packet.get(TcpPacket.class);
                                TcpPacket.TcpHeader tcpHeader = tcpPacket.getHeader();
                                if (tcpHeader.getRst()) {
                                                flag = "RST";
                                } else if (tcpHeader.getFin()) {
                                                flag = "FIN";
                                } else if (tcpHeader.getSyn()) {
                                                flag = "SYN";
                                } else if (tcpHeader.getAck()) {
                                                flag = "ACK";
                                }
                }
                return "P_" + sourceIp + "_" + destIp + "_" + proto + "_" + flag + "_" + timestamp;
        }

        @Override
        public void stop() throws Exception {
                running.set(false);
                if (consumer != null)
                        consumer.close();
                if (clickhouseConn != null)
                        clickhouseConn.close();
                Logger logger = LoggerFactory.getLogger(ClickHousePacketVerticle.class);
                logger.info(Colors.RED + "[ CLICKHOUSE PACKET VERTICLE ]    ClickHousePacketVerticle stopped!"
                                + Colors.RESET);
        }
}
