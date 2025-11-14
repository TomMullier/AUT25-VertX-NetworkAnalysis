package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;

import org.apache.kafka.clients.consumer.*;
import org.pcap4j.packet.IpPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aut25.vertx.utils.Colors;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.pcap4j.packet.Packet;
import org.pcap4j.packet.TcpPacket;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.pcap4j.packet.EthernetPacket;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

public class BenchmarkVerticle extends AbstractVerticle {

        private static final Logger logger = LoggerFactory.getLogger(BenchmarkVerticle.class);
        KafkaConsumer<String, String> consumer;
        String mode;
        private final AtomicBoolean running = new AtomicBoolean(true);// Analyse avec nDPI
        private int lineCount_packet = 0;

        @Override
        public void start() throws Exception {
                logger.info(Colors.GREEN + "[ ANALYSE VERTICLE ]              Starting BenchmarkVerticle..."
                                + Colors.RESET);
                //! readPackets_to_CSV();
        }

        /**
         * Read packets from Kafka topic "network-data" and write specific fields to a CSV file.
         */
        private void readPackets_to_CSV() {

                // Config Kafka
                Map<String, String> config = new HashMap<>();
                config.put("bootstrap.servers", "localhost:9092");
                config.put("group.id", "analyse-group");
                config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                config.put("auto.offset.reset", "earliest");
                config.put("enable.auto.commit", "false");
                config.put("max.poll.interval.ms", "600000");
                config.put("session.timeout.ms", "30000");
                config.put("heartbeat.interval.ms", "10000");

                KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);

                consumer.subscribe("network-data")
                                .onSuccess(v -> logger.info("[ANALYSE VERTICLE] Subscribed to topic network-data"))
                                .onFailure(err -> logger
                                                .error("[ANALYSE VERTICLE] Failed to subscribe: " + err.getMessage()));

                // CSV setup
                Path csvPath = Paths.get("_tests/benchmark/01_phase/csv/plateform_analyse_output.csv");
                BufferedWriter writer;
                try {
                        Files.createDirectories(csvPath.getParent());
                        writer = Files.newBufferedWriter(csvPath, StandardOpenOption.CREATE,
                                        StandardOpenOption.TRUNCATE_EXISTING);
                        lineCount_packet = 0;
                } catch (IOException e) {
                        logger.error("Failed to open CSV file: " + e.getMessage());
                        return;
                }

                AtomicInteger frameCounter = new AtomicInteger(1); // frame.number equivalent

                consumer.handler(record -> {
                        vertx.setTimer(200, tid -> {
                                if (!running.get())
                                        return;

                                String value = record.value();
                                if (value == null || value.isEmpty() || value.equals("reset")) {
                                        logger.warn("[ANALYSE VERTICLE] Received empty record, skipping.");
                                        return;
                                }

                                try {
                                        JsonObject json = new JsonObject(value);
                                        String srcIp = "";
                                        String dstIp = "";
                                        String protocol = "UNKNOWN";
                                        int sport = 0;
                                        int dport = 0;
                                        int length = 0;

                                        String rawPacketBase64 = json.getString("rawPacket");
                                        if (rawPacketBase64 != null) {
                                                byte[] rawData = Base64.getDecoder().decode(rawPacketBase64);
                                                Packet packet = EthernetPacket.newPacket(rawData, 0, rawData.length);
                                                length = packet.length();

                                                if (packet.contains(IpPacket.class)) {
                                                        IpPacket ipPacket = packet.get(IpPacket.class);
                                                        srcIp = ipPacket.getHeader().getSrcAddr().getHostAddress();
                                                        dstIp = ipPacket.getHeader().getDstAddr().getHostAddress();

                                                        if (ipPacket.contains(TcpPacket.class)) {
                                                                TcpPacket tcp = ipPacket.get(TcpPacket.class);
                                                                sport = tcp.getHeader().getSrcPort().valueAsInt();
                                                                dport = tcp.getHeader().getDstPort().valueAsInt();
                                                        }
                                                }
                                        }

                                        // Write CSV line
                                        try {
                                                writer.write(frameCounter.getAndIncrement() + "," +
                                                                srcIp + "," +
                                                                dstIp + "," +
                                                                sport + "," +
                                                                dport + "," +
                                                                length);
                                                writer.newLine();
                                                lineCount_packet++;
                                                writer.flush();
                                        } catch (IOException e) {
                                                logger.error("Failed to write CSV line: " + e.getMessage());
                                        }

                                        if (lineCount_packet == 10000) {
                                                logger.info("[ANALYSE VERTICLE] Written {} lines to CSV.",
                                                                lineCount_packet);
                                                // Stop program
                                                running.set(false);
                                                // emulate ctrl-c
                                                vertx.close();
                                                return;
                                        }

                                } catch (Exception e) {
                                        logger.error("[ANALYSE VERTICLE] Failed to parse JSON or packet: "
                                                        + e.getMessage());
                                }

                                // Commit manuel async
                                consumer.commit()
                                                .onSuccess(v -> logger.debug("[ANALYSE VERTICLE] Offsets committed."))
                                                .onFailure(err -> logger.error("[ANALYSE VERTICLE] Commit failed: "
                                                                + err.getMessage()));
                        });
                });
        }

        @Override
        public void stop() throws Exception {
                running.set(false);
                if (consumer != null)
                        consumer.close();

                logger.info(Colors.RED + "[ ANALYSE VERTICLE ]              AnalyseVerticle stopped!" + Colors.RESET);
        }

}
