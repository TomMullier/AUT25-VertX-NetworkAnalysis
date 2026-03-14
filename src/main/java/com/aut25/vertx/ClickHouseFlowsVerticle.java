package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.IpPacket;
import org.pcap4j.packet.Packet;
import io.vertx.core.json.JsonArray;

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

public class ClickHouseFlowsVerticle extends AbstractVerticle {

        private Connection clickhouseConn;
        private KafkaConsumer<String, String> consumer;
        private long delay = 500; // Délai entre les polls Kafka en ms

        private final AtomicBoolean running = new AtomicBoolean(true);
        private final List<JsonObject> buffer = new ArrayList<>();
        private List<JsonObject> inflightBatch = null;
        private final AtomicBoolean flushing = new AtomicBoolean(false);

        private static final int FLUSH_INTERVAL_MS = 1000;
        private static final int MAX_BATCH_SIZE = 200; // tu peux ajuster

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

                config.put("max.poll.records", "2000"); // nombre max de messages par poll
                config.put("fetch.max.wait.ms", "100"); // attendre max 100ms si fetch.min.bytes non rempli
                config.put("fetch.min.bytes", "1"); // fetch minimal 1 octet (ou 32KB si gros)
                config.put("max.partition.fetch.bytes", "2097152"); // 2MB max par partition
                config.put("session.timeout.ms", "10000"); // timeout session consumer
                config.put("heartbeat.interval.ms", "3000"); // interval heartbeat

                consumer = KafkaConsumer.create(vertx, config);

                consumer.handler(record -> {
                        if (!running.get())
                                return;

                        String value = record.value();
                        if (value == null || value.isEmpty() || value.equals("reset") || value.equals("PCAP_DONE"))
                                return;

                        try {
                                buffer.add(new JsonObject(value));
                        } catch (Exception e) {
                                logger.error("Invalid JSON received: {}", e.getMessage());
                        }

                        if (buffer.size() >= MAX_BATCH_SIZE) {
                                flushBatch();
                        }
                });

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

                // Flush périodique
                vertx.setPeriodic(FLUSH_INTERVAL_MS, id -> flushBatch());

                logger.debug("[ CLICKHOUSE FLOWS VERTICLE ]     ClickHouseFlowsVerticle deployed successfully!");
        }

        private void flushBatch() {
                if (!running.get() || flushing.get() || buffer.isEmpty())
                        return;

                flushing.set(true);
                inflightBatch = new ArrayList<>(buffer);
                buffer.clear();

                consumer.pause();

                vertx.executeBlocking(promise -> {
                        try {
                                // Insert into ClickHouse
                                String insertSQL = "INSERT INTO network_flows " +
                                                "(id, firstSeen, lastSeen, srcIp, dstIp, srcPort, dstPort, protocol, bytes, packets, durationMs, flowKey, treatmentDelay, "
                                                +
                                                "minPacketLength, maxPacketLength, meanPacketLength, stddevPacketLength, "
                                                +
                                                "bytesPerSecond, packetsPerSecond, " +
                                                "totalBytesUpstream, totalBytesDownstream, totalPacketsUpstream, totalPacketsDownstream, "
                                                +
                                                "ratioBytesUpDown, ratioPacketsUpDown, flowDurationMs, " +
                                                "interArrivalTimeMean, interArrivalTimeStdDev, interArrivalTimeMin, interArrivalTimeMax, "
                                                +
                                                "flowSymmetry, synRate, finRate, rstRate, ackRate, pshRate, synCount, finCount, rstCount, ackCount, pshCount, "
                                                +
                                                "tcpFraction, udpFraction, otherFraction,"
                                                + 
                                                // "appProtocolBytes, appProtocol, ndpiFlowPtr, riskLevel, riskMask, riskLabel, riskSeverity, "+
                                                "packetSummaries, reasonOfFlowEnd, srcCountry, dstCountry, srcDomain, dstDomain, srcOrg, dstOrg) "
                                                +
                                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                                try (PreparedStatement pstmt = clickhouseConn.prepareStatement(insertSQL)) {
                                        for (JsonObject json : inflightBatch) {

                                                String flowId = UUID.randomUUID().toString();
                                                long firstSeen = json.getLong("firstSeen", System.currentTimeMillis());
                                                long lastSeen = json.getLong("lastSeen", firstSeen);
                                                String srcIp = json.getString("srcIp", "0.0.0.0");
                                                String dstIp = json.getString("dstIp", "0.0.0.0");
                                                String srcPort = json.getString("srcPort");
                                                String dstPort = json.getString("dstPort");

                                                String protocol = json.getString("protocol", "UNKNOWN");
                                                long bytes = json.getLong("bytes", 0L);
                                                long packetCount = json.getLong("packetCount", 0L);
                                                long durationMs = json.getLong("durationMs", lastSeen - firstSeen);
                                                String flowKey = json.getString("flowKey", "N/A");

                                                // Additional fields
                                                double minPacketLength = getSafeDouble(json, "minPacketLength", 0.0);
                                                double maxPacketLength = getSafeDouble(json, "maxPacketLength", 0.0);
                                                double meanPacketLength = getSafeDouble(json, "meanPacketLength", 0.0);
                                                double stddevPacketLength = getSafeDouble(json, "stddevPacketLength",
                                                                0.0);

                                                double bytesPerSecond = getSafeDouble(json, "bytesPerSecond", 0.0);
                                                double packetsPerSecond = getSafeDouble(json, "packetsPerSecond", 0.0);

                                                double totalBytesUpstream = getSafeDouble(json, "totalBytesUpstream",
                                                                0.0);
                                                double totalBytesDownstream = getSafeDouble(json,
                                                                "totalBytesDownstream", 0.0);
                                                double totalPacketsUpstream = getSafeDouble(json,
                                                                "totalPacketsUpstream", 0.0);
                                                double totalPacketsDownstream = getSafeDouble(json,
                                                                "totalPacketsDownstream",
                                                                0.0);
                                                double ratioBytesUpDown = getSafeDouble(json, "ratioBytesUpDown", 0.0);
                                                double ratioPacketsUpDown = getSafeDouble(json, "ratioPacketsUpDown",
                                                                0.0);

                                                long flowDurationMs = json.getLong("flowDurationMs", durationMs);

                                                double interArrivalTimeMean = getSafeDouble(json,
                                                                "interArrivalTimeMean", 0.0);
                                                double interArrivalTimeStdDev = getSafeDouble(json,
                                                                "interArrivalTimeStdDev",
                                                                0.0);
                                                double interArrivalTimeMin = getSafeDouble(json, "interArrivalTimeMin",
                                                                0.0);
                                                double interArrivalTimeMax = getSafeDouble(json, "interArrivalTimeMax",
                                                                0.0);

                                                double flowSymmetry = getSafeDouble(json, "flowSymmetry", 0.0);
                                                double synRate = getSafeDouble(json, "synRate", 0.0);
                                                double finRate = getSafeDouble(json, "finRate", 0.0);
                                                double rstRate = getSafeDouble(json, "rstRate", 0.0);
                                                double ackRate = getSafeDouble(json, "ackRate", 0.0);
                                                double pshRate = getSafeDouble(json, "pshRate", 0.0);
                                                double synCount = getSafeDouble(json, "synCount", 0.0);
                                                double finCount = getSafeDouble(json, "finCount", 0.0);
                                                double rstCount = getSafeDouble(json, "rstCount", 0.0);
                                                double ackCount = getSafeDouble(json, "ackCount", 0.0);
                                                double pshCount = getSafeDouble(json, "pshCount", 0.0);

                                                double tcpFraction = getSafeDouble(json, "tcpFraction", 0.0);
                                                double udpFraction = getSafeDouble(json, "udpFraction", 0.0);
                                                double otherFraction = getSafeDouble(json, "otherFraction", 0.0);

                                                // double appProtocolBytes = getSafeDouble(json, "appProtocolBytes", 0.0);
                                                // String appProtocol = json.getString("appProtocol", "UNKNOWN");
                                                // long ndpiFlowPtr = json.getLong("ndpiFlowPtr", 0L);

                                                // int riskLevel = json.getInteger("riskLevel", -1);
                                                // int riskMask = json.getInteger("riskMask", -1);
                                                // String riskLabel = json.getString("riskLabel", "UNKNOWN");
                                                // String[] riskLabels = riskLabel.isEmpty() ? new String[0]
                                                //                 : riskLabel.split(",");
                                                // String riskSeverity = json.getString("riskSeverity", "UNKNOWN");

                                                String packetSummariesString = json.getString("packetSummariesString",
                                                                "");
                                                String[] packetSummaries = packetSummariesString.isEmpty()
                                                                ? new String[0]
                                                                : packetSummariesString.split(",");

                                                String reasonOfFlowEnd = json.getString("reasonOfFlowEnd", "");

                                                String srcCountry = json.getString("srcCountry", "N/A");
                                                String dstCountry = json.getString("dstCountry", "N/A");
                                                String srcDomain = json.getString("srcDomain", "N/A");
                                                String dstDomain = json.getString("dstDomain", "N/A");
                                                String srcOrg = json.getString("srcOrg", "N/A");
                                                String dstOrg = json.getString("dstOrg", "N/A");

                                                JsonArray treatmentDelayJson = json.getJsonArray("treatmentDelay");
                                                long[] treatmentDelayStrs = treatmentDelayJson
                                                                .stream()
                                                                .mapToLong(o -> ((Number) o).longValue())
                                                                .toArray();

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
                                                pstmt.setObject(13, treatmentDelayStrs);

                                                pstmt.setDouble(14, minPacketLength);
                                                pstmt.setDouble(15, maxPacketLength);
                                                pstmt.setDouble(16, meanPacketLength);
                                                pstmt.setDouble(17, stddevPacketLength);

                                                pstmt.setDouble(18, bytesPerSecond);
                                                pstmt.setDouble(19, packetsPerSecond);

                                                pstmt.setDouble(20, totalBytesUpstream);
                                                pstmt.setDouble(21, totalBytesDownstream);
                                                pstmt.setDouble(22, totalPacketsUpstream);
                                                pstmt.setDouble(23, totalPacketsDownstream);
                                                pstmt.setDouble(24, ratioBytesUpDown);
                                                pstmt.setDouble(25, ratioPacketsUpDown);

                                                pstmt.setLong(26, flowDurationMs);

                                                pstmt.setDouble(27, interArrivalTimeMean);
                                                pstmt.setDouble(28, interArrivalTimeStdDev);
                                                pstmt.setDouble(29, interArrivalTimeMin);
                                                pstmt.setDouble(30, interArrivalTimeMax);

                                                pstmt.setDouble(31, flowSymmetry);
                                                pstmt.setDouble(32, synRate);
                                                pstmt.setDouble(33, finRate);
                                                pstmt.setDouble(34, rstRate);
                                                pstmt.setDouble(35, ackRate);
                                                pstmt.setDouble(36, pshRate);
                                                pstmt.setDouble(37, synCount);
                                                pstmt.setDouble(38, finCount);
                                                pstmt.setDouble(39, rstCount);
                                                pstmt.setDouble(40, ackCount);
                                                pstmt.setDouble(41, pshCount);

                                                pstmt.setDouble(42, tcpFraction);
                                                pstmt.setDouble(43, udpFraction);
                                                pstmt.setDouble(44, otherFraction);

                                                

                                                pstmt.setString(45, setArrayAsClickHouseStringArray(packetSummaries));

                                                pstmt.setString(46, reasonOfFlowEnd);

                                                pstmt.setString(47, srcCountry);
                                                pstmt.setString(48, dstCountry);
                                                pstmt.setString(49, srcDomain);
                                                pstmt.setString(50, dstDomain);
                                                pstmt.setString(51, srcOrg);
                                                pstmt.setString(52, dstOrg);

                                                pstmt.addBatch();
                                        }

                                        pstmt.executeBatch();
                                }
                                promise.complete();
                        } catch (Exception e) {
                                promise.fail(e);
                        }
                }, ar -> {
                        Logger logger = LoggerFactory.getLogger(ClickHouseFlowsVerticle.class);
                        if (ar.succeeded()) {
                                // Commit Kafka après succès ClickHouse
                                consumer.commit(commitAr -> {
                                        if (commitAr.failed()) {
                                                logger.error("Kafka commit failed: {}", commitAr.cause().getMessage());
                                                buffer.addAll(inflightBatch); // retry batch
                                        }
                                        inflightBatch = null;
                                        flushing.set(false);
                                        consumer.resume();
                                });
                        } else {
                                logger.error("ClickHouse batch insert failed: {}", ar.cause().getMessage());
                                buffer.addAll(inflightBatch); // retry
                                inflightBatch = null;
                                flushing.set(false);
                                vertx.setTimer(1000, id -> consumer.resume());
                        }
                });
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
