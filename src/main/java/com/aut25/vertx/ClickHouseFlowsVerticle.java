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
                logger.info(Colors.GREEN + "[ CLICKHOUSE FLOWS VERTICLE ]     Starting ClickHouseFlowsVerticle..."
                                + Colors.RESET);

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
                                long srcPort = Long.parseLong(json.getString("srcPort", "0"));
                                long dstPort = Long.parseLong(json.getString("dstPort", "0"));
                                String protocol = json.getString("protocol", "UNKNOWN");
                                long bytes = json.getLong("bytes", 0L);
                                long packetCount = json.getLong("packetCount", 0L);
                                long durationMs = json.getLong("durationMs", lastSeen - firstSeen);
                                String flowKey = json.getString("flowKey", "N/A");

                                // Additional fields
                                double minPacketLength = getSafeDouble(json, "minPacketLength", 0.0);
                                double maxPacketLength = getSafeDouble(json, "maxPacketLength", 0.0);
                                double meanPacketLength = getSafeDouble(json, "meanPacketLength", 0.0);
                                double stddevPacketLength = getSafeDouble(json, "stddevPacketLength", 0.0);

                                double bytesPerSecond = getSafeDouble(json, "bytesPerSecond", 0.0);
                                double packetsPerSecond = getSafeDouble(json, "packetsPerSecond", 0.0);

                                double totalBytesUpstream = getSafeDouble(json, "totalBytesUpstream", 0.0);
                                double totalBytesDownstream = getSafeDouble(json, "totalBytesDownstream", 0.0);
                                double totalPacketsUpstream = getSafeDouble(json, "totalPacketsUpstream", 0.0);
                                double totalPacketsDownstream = getSafeDouble(json, "totalPacketsDownstream", 0.0);
                                double ratioBytesUpDown = getSafeDouble(json, "ratioBytesUpDown", 0.0);
                                double ratioPacketsUpDown = getSafeDouble(json, "ratioPacketsUpDown", 0.0);

                                long flowDurationMs = json.getLong("flowDurationMs", durationMs);

                                double interArrivalTimeMean = getSafeDouble(json, "interArrivalTimeMean", 0.0);
                                double interArrivalTimeStdDev = getSafeDouble(json, "interArrivalTimeStdDev", 0.0);
                                double interArrivalTimeMin = getSafeDouble(json, "interArrivalTimeMin", 0.0);
                                double interArrivalTimeMax = getSafeDouble(json, "interArrivalTimeMax", 0.0);

                                double flowSymmetry = getSafeDouble(json, "flowSymmetry", 0.0);
                                double synRate = getSafeDouble(json, "synRate", 0.0);
                                double finRate = getSafeDouble(json, "finRate", 0.0);
                                double rstRate = getSafeDouble(json, "rstRate", 0.0);
                                double ackRate = getSafeDouble(json, "ackRate", 0.0);

                                double tcpFraction = getSafeDouble(json, "tcpFraction", 0.0);
                                double udpFraction = getSafeDouble(json, "udpFraction", 0.0);
                                double otherFraction = getSafeDouble(json, "otherFraction", 0.0);

                                double appProtocolBytes = getSafeDouble(json, "appProtocolBytes", 0.0);
                                String appProtocol = json.getString("appProtocol", "UNKNOWN");
                                long ndpiFlowPtr = json.getLong("ndpiFlowPtr", 0L);

                                int riskLevel = json.getInteger("riskLevel", -1);
                                int riskMask = json.getInteger("riskMask", -1);
                                String riskLabel = json.getString("riskLabel", "UNKNOWN");
                                String[] riskLabels = riskLabel.isEmpty() ? new String[0] : riskLabel.split(",");
                                String riskSeverity = json.getString("riskSeverity", "UNKNOWN");

                                String packetSummariesString = json.getString("packetSummariesString", "");
                                String[] packetSummaries = packetSummariesString.isEmpty() ? new String[0]
                                                : packetSummariesString.split(",");

                                String reasonOfFlowEnd = json.getString("reasonOfFlowEnd", "");

                                // Insert into ClickHouse
                                String insertSQL = "INSERT INTO network_flows " +
                                                "(id, firstSeen, lastSeen, srcIp, dstIp, srcPort, dstPort, protocol, bytes, packets, durationMs, flowKey, "
                                                +
                                                "minPacketLength, maxPacketLength, meanPacketLength, stddevPacketLength, "
                                                +
                                                "bytesPerSecond, packetsPerSecond, " +
                                                "totalBytesUpstream, totalBytesDownstream, totalPacketsUpstream, totalPacketsDownstream, "
                                                +
                                                "ratioBytesUpDown, ratioPacketsUpDown, flowDurationMs, " +
                                                "interArrivalTimeMean, interArrivalTimeStdDev, interArrivalTimeMin, interArrivalTimeMax, "
                                                +
                                                "flowSymmetry, synRate, finRate, rstRate, ackRate, " +
                                                "tcpFraction, udpFraction, otherFraction, appProtocolBytes, appProtocol, ndpiFlowPtr, riskLevel, riskMask, riskLabel, riskSeverity, packetSummaries, reasonOfFlowEnd) "
                                                +
                                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                                try (PreparedStatement pstmt = clickhouseConn.prepareStatement(insertSQL)) {
                                        pstmt.setString(1, flowId);
                                        pstmt.setLong(2, firstSeen);
                                        pstmt.setLong(3, lastSeen);
                                        pstmt.setString(4, srcIp);
                                        pstmt.setString(5, dstIp);
                                        pstmt.setLong(6, srcPort);
                                        pstmt.setLong(7, dstPort);
                                        pstmt.setString(8, protocol);
                                        pstmt.setLong(9, bytes);
                                        pstmt.setLong(10, packetCount);
                                        pstmt.setLong(11, durationMs);
                                        pstmt.setString(12, flowKey);

                                        pstmt.setDouble(13, minPacketLength);
                                        pstmt.setDouble(14, maxPacketLength);
                                        pstmt.setDouble(15, meanPacketLength);
                                        pstmt.setDouble(16, stddevPacketLength);

                                        pstmt.setDouble(17, bytesPerSecond);
                                        pstmt.setDouble(18, packetsPerSecond);

                                        pstmt.setDouble(19, totalBytesUpstream);
                                        pstmt.setDouble(20, totalBytesDownstream);
                                        pstmt.setDouble(21, totalPacketsUpstream);
                                        pstmt.setDouble(22, totalPacketsDownstream);
                                        pstmt.setDouble(23, ratioBytesUpDown);
                                        pstmt.setDouble(24, ratioPacketsUpDown);

                                        pstmt.setLong(25, flowDurationMs);

                                        pstmt.setDouble(26, interArrivalTimeMean);
                                        pstmt.setDouble(27, interArrivalTimeStdDev);
                                        pstmt.setDouble(28, interArrivalTimeMin);
                                        pstmt.setDouble(29, interArrivalTimeMax);

                                        pstmt.setDouble(30, flowSymmetry);
                                        pstmt.setDouble(31, synRate);
                                        pstmt.setDouble(32, finRate);
                                        pstmt.setDouble(33, rstRate);
                                        pstmt.setDouble(34, ackRate);

                                        pstmt.setDouble(35, tcpFraction);
                                        pstmt.setDouble(36, udpFraction);
                                        pstmt.setDouble(37, otherFraction);

                                        pstmt.setDouble(38, appProtocolBytes);
                                        pstmt.setString(39, appProtocol);
                                        pstmt.setLong(40, ndpiFlowPtr);

                                        pstmt.setInt(41, riskLevel);
                                        pstmt.setInt(42, riskMask);
                                        pstmt.setString(43, setArrayAsClickHouseStringArray(riskLabels));
                                        pstmt.setString(44, riskSeverity);

                                        pstmt.setString(45, setArrayAsClickHouseStringArray(packetSummaries));

                                        pstmt.setString(46, reasonOfFlowEnd);

                                        pstmt.executeUpdate();
                                }

                        } catch (Exception e) {
                                logger.error("[ CLICKHOUSE FLOWS VERTICLE ]     Error processing record: {}",
                                                e.getMessage());

                        }

                        // Commit manuel après traitement
                        consumer.commit(ar -> {
                                if (ar.failed()) {

                                        if (ar.cause().getMessage() != null) {
                                                logger.error(
                                                                "[ CLICKHOUSE FLOWS VERTICLE ]     Commit failed: "
                                                                                + ar.cause().getMessage());
                                        }

                                }
                        });
                });

                // Subscribe to topic
                consumer.subscribe("network-flows", ar -> {
                        if (ar.succeeded()) {
                                logger.info(Colors.CYAN
                                                + "[ CLICKHOUSE FLOWS VERTICLE ]     Subscribed to topic network-flows"
                                                + Colors.RESET);
                        } else {
                                logger.error("[ CLICKHOUSE FLOWS VERTICLE ]     Failed to subscribe: {}",
                                                ar.cause().getMessage());
                        }
                });

                logger.debug("[ CLICKHOUSE FLOWS VERTICLE ]     ClickHouseFlowsVerticle deployed successfully!");
        }

        private String setArrayAsClickHouseStringArray(String[] array) {
                StringBuilder sb = new StringBuilder();
                sb.append("[");
                for (int i = 0; i < array.length; i++) {
                        String element = array[i].startsWith(" ") ? array[i].substring(1) : array[i];
                        sb.append("'").append(element).append("'");
                        if (i < array.length - 1) {
                                sb.append(",");
                        }
                }
                sb.append("]");
                return sb.toString();
        }

        @Override
        public void stop() throws Exception {
                running.set(false);
                if (consumer != null)
                        consumer.close();
                if (clickhouseConn != null)
                        clickhouseConn.close();
                Logger logger = LoggerFactory.getLogger(ClickHouseFlowsVerticle.class);
                logger.info(Colors.RED + "[ CLICKHOUSE FLOWS VERTICLE ]     ClickHouseFlowsVerticle stopped."
                                + Colors.RESET);
        }

        private double getSafeDouble(JsonObject json, String field, double defaultValue) {
                Object value = json.getValue(field);
                if (value == null) {
                        return defaultValue;
                }
                if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                }
                if (value instanceof String) {
                        String s = ((String) value).trim();
                        if (s.equalsIgnoreCase("Infinity"))
                                return Double.POSITIVE_INFINITY;
                        if (s.equalsIgnoreCase("-Infinity"))
                                return Double.NEGATIVE_INFINITY;
                        if (s.equalsIgnoreCase("NaN"))
                                return Double.NaN;
                        try {
                                return Double.parseDouble(s);
                        } catch (NumberFormatException e) {
                                return defaultValue;
                        }
                }
                return defaultValue;
        }

}
