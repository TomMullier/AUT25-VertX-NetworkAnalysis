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
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aut25.vertx.utils.Colors;

public class ClickHousePacketVerticle extends AbstractVerticle {

        private Connection clickhouseConn;
        private KafkaConsumer<String, String> consumer;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final List<JsonObject> buffer = new ArrayList<>();
        private List<JsonObject> inflightBatch = null;

        private static final int FLUSH_INTERVAL_MS = 1000;
        private static final int MAX_BATCH_SIZE = 500;

        private final AtomicBoolean flushing = new AtomicBoolean(false);

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

                logger.debug("[ CLICKHOUSE PACKET VERTICLE ]    Connected to ClickHouse at " + jdbcUrl);
                logger.debug(
                                "[ CLICKHOUSE PACKET VERTICLE ]    KafkaConsumer configured for topic network-data : "
                                                + config.toString());

                // Handler pour chaque message reçu
                consumer.handler(record -> {
                        if (!running.get())
                                return;

                        String value = record.value();
                        if (value == null || value.isEmpty() || value.equals("reset") || value.equals("PCAP_DONE"))
                                return;

                        try {
                                buffer.add(new JsonObject(value));
                        } catch (Exception e) {
                                logger.error("Invalid JSON received", e);
                        }

                        if (buffer.size() >= MAX_BATCH_SIZE) {
                                flushBatch();
                        }
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
                vertx.setPeriodic(FLUSH_INTERVAL_MS, id -> flushBatch());

                logger.debug("[ CLICKHOUSE PACKET VERTICLE ]    ClickHousePacketVerticle deployed successfully!");
        }

        private void flushBatch() {

                if (!running.get())
                        return;

                // Un seul flush à la fois
                if (flushing.get())
                        return;

                if (buffer.isEmpty())
                        return;

                flushing.set(true);

                inflightBatch = new ArrayList<>(buffer);
                buffer.clear();

                consumer.pause();

                vertx.executeBlocking(promise -> {
                        try {
                                String sql = "INSERT INTO network_data "
                                                + "(id, timestamp, srcIp, dstIp, protocol, bytes, rawPacket) "
                                                + "VALUES (?, ?, ?, ?, ?, ?, ?)";

                                try (PreparedStatement stmt = clickhouseConn.prepareStatement(sql)) {
                                        for (JsonObject json : inflightBatch) {

                                                long timestamp = json.getLong("timestamp", System.currentTimeMillis());
                                                String rawPacket = json.getString("rawPacket");

                                                if (rawPacket == null)
                                                        continue;

                                                byte[] rawData = Base64.getDecoder().decode(rawPacket);
                                                Packet packet = EthernetPacket.newPacket(rawData, 0, rawData.length);

                                                String srcIp = "";
                                                String dstIp = "";
                                                String protocol = "UNKNOWN";

                                                if (packet.contains(IpPacket.class)) {
                                                        IpPacket ip = packet.get(IpPacket.class);
                                                        srcIp = ip.getHeader().getSrcAddr().getHostAddress();
                                                        dstIp = ip.getHeader().getDstAddr().getHostAddress();
                                                        protocol = ip.getHeader().getProtocol() != null
                                                                        ? ip.getHeader().getProtocol().name()
                                                                        : "UNKNOWN";
                                                }

                                                stmt.setString(1,
                                                                setPacketId(srcIp, dstIp, protocol, timestamp, packet));
                                                stmt.setLong(2, timestamp);
                                                stmt.setString(3, srcIp);
                                                stmt.setString(4, dstIp);
                                                stmt.setString(5, protocol);
                                                stmt.setInt(6, packet.length());
                                                stmt.setString(7, rawPacket);
                                                stmt.addBatch();
                                        }

                                        stmt.executeBatch();
                                }

                                promise.complete();

                        } catch (Exception e) {
                                promise.fail(e);
                        }
                }, ar -> {

                        if (ar.succeeded()) {
                                // Commit Kafka UNIQUEMENT après succès ClickHouse
                                consumer.commit(commitAr -> {
                                        if (commitAr.failed()) {
                                                // Commit raté → on remet le batch
                                                buffer.addAll(inflightBatch);
                                        }
                                        inflightBatch = null;
                                        flushing.set(false);
                                        consumer.resume();
                                });

                        } else {
                                // Échec ClickHouse → retry plus tard
                                buffer.addAll(inflightBatch);
                                inflightBatch = null;
                                flushing.set(false);

                                vertx.setTimer(1000, id -> consumer.resume());
                        }
                });
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
                String flag = "";
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
