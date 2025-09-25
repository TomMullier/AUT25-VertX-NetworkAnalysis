package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.IpPacket;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.TcpPacket;
import org.pcap4j.packet.UdpPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.sleep;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class FlowAggregatorVerticle extends AbstractVerticle {

        private static final Logger logger = LoggerFactory.getLogger(FlowAggregatorVerticle.class);

        // Kafka config
        private static final String BOOTSTRAP_SERVERS = "localhost:9092";
        private static final String IN_TOPIC = "network-data";
        private static final String OUT_TOPIC = "network-flows";
        private static final String GROUP_ID = "flow-aggregator-group";

        // Flow timing (ms) - à ajuster
        private static final long FLOW_INACTIVITY_TIMEOUT_MS = 60_000; // flush si inactif > 1min
        private static final long FLOW_MAX_AGE_MS = 600_000; // flush si age > 10min
        private static final long KAFKA_POLL_MS = 200; // durée poll Kafka
        private static final long FLOW_CLEAN_PERIOD_MS = 5_000; // vérification périodique flows

        private Consumer<String, String> consumer;
        private Producer<String, String> producer;

        // map clé -> Flow
        private final Map<String, Flow> flows = new ConcurrentHashMap<>();

        // flag pour shutdown propre
        private final AtomicBoolean running = new AtomicBoolean(true);

        @Override
        public void start() throws Exception {
                logger.info(Colors.GREEN + "[ FLOWAGGREGATOR VERTICLE ] Starting FlowAggregatorVerticle..."
                                + Colors.RESET);

                // Init Kafka consumer
                Properties cprops = new Properties();
                cprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
                cprops.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
                cprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                cprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                cprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                // désactiver auto commit pour mieux contrôler les offsets
                cprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
                // si traitement lourd, augmenter max.poll.interval.ms
                cprops.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");

                consumer = new KafkaConsumer<>(cprops);
                consumer.subscribe(Collections.singletonList(IN_TOPIC));

                // Init Kafka producer
                Properties pprops = new Properties();
                pprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
                pprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                pprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                // optional acks
                pprops.put(ProducerConfig.ACKS_CONFIG, "1");

                producer = new KafkaProducer<>(pprops);

                logger.info("[ FLOWAGGREGATOR VERTICLE ] Kafka consumer and producer initialized.");

                // Periodic task to poll kafka and process messages (runs on Vert.x event loop)
                vertx.setPeriodic(KAFKA_POLL_MS, id -> {
                        if (!running.get())
                                return;
                        // poll in blocking to avoid blocking event-loop too long: use executeBlocking
                        vertx.executeBlocking(promise -> {
                                try {
                                        ConsumerRecords<String, String> records = consumer
                                                        .poll(Duration.ofMillis(KAFKA_POLL_MS));
                                        if (!records.isEmpty()) {
                                                // process records
                                                for (ConsumerRecord<String, String> rec : records) {
                                                        if (rec.value() == null || rec.value().isEmpty()
                                                                        || rec.value().equals("reset")) {
                                                                logger.warn("[ FLOWAGGREGATOR VERTICLE ] Received empty record, skipping.");
                                                                continue;
                                                        }
                                                        try {
                                                                JsonObject json = new JsonObject(rec.value());
                                                                try {
                                                                        // Process the record to update/create flow
                                                                        logger.debug(
                                                                                        "[ FLOWAGGREGATOR VERTICLE ] Processing record from Kafka: "
                                                                                                        + json.encodePrettily());
                                                                        processRecord(json);
                                                                } catch (Exception e) {
                                                                        logger.warn("[ FLOWAGGREGATOR VERTICLE ] Could not parse IP/transport layer: {}",
                                                                                        e.getMessage());
                                                                }
                                                        } catch (Exception e) {
                                                                // log and continue (so we don't lose other messages)
                                                                logger.error(
                                                                                "[ FLOWAGGREGATOR VERTICLE ] Error processing record: "
                                                                                                + e.getMessage());
                                                                e.printStackTrace();
                                                        }
                                                }
                                                // once all records processed, commit offsets
                                                try {
                                                        consumer.commitSync();
                                                } catch (Exception e) {
                                                        logger.error("[ FLOWAGGREGATOR VERTICLE ] Error committing offsets: "
                                                                        + e.getMessage());
                                                        e.printStackTrace();
                                                }
                                        }
                                        promise.complete();
                                } catch (Exception ex) {
                                        logger.error("[ FLOWAGGREGATOR VERTICLE ] Error polling Kafka: "
                                                        + ex.getMessage());
                                        ex.printStackTrace();
                                        promise.fail(ex);
                                }
                        }, res -> {
                                if (res.failed()) {
                                        // log error -- but do not crash Verticle
                                        res.cause().printStackTrace();
                                        logger.error("[ FLOWAGGREGATOR VERTICLE ] Error polling Kafka: "
                                                        + res.cause().getMessage());
                                        res.cause().printStackTrace();
                                }
                        });
                });

                // Periodic cleaner to flush expired flows
                vertx.setPeriodic(FLOW_CLEAN_PERIOD_MS, id -> {
                        if (!running.get())
                                return;
                        long now = System.currentTimeMillis();
                        List<Flow> toFlush = new ArrayList<>();
                        for (Flow f : flows.values()) {
                                if (now - f.lastSeen >= FLOW_INACTIVITY_TIMEOUT_MS
                                                || now - f.firstSeen >= FLOW_MAX_AGE_MS) {
                                        // Move to flush list and remove from map
                                        if (flows.remove(f.key) != null) {
                                                toFlush.add(f);
                                        }
                                }
                        }
                        // publish flushed flows
                        for (Flow f : toFlush) {
                                logger.debug(String.format(
                                                "[ FLOWAGGREGATOR VERTICLE ] Flushing flow: key=%s bytes=%d packets=%d durationMs=%d",
                                                f.key, f.bytes, f.packetCount, (f.lastSeen - f.firstSeen)));
                                publishFlow(f);
                        }

                        logger.info("[ FLOWAGGREGATOR VERTICLE ] Flushed " + toFlush.size() + " flows.");
                        logger.info("[ FLOWAGGREGATOR VERTICLE ] Active flows count: " + flows.size());

                        logger.debug("[ FLOWAGGREGATOR VERTICLE ] Flow cleanup completed at "
                                        + System.currentTimeMillis());
                });

                logger.info("[ FLOWAGGREGATOR VERTICLE ] FlowAggregatorVerticle started.");
        }

        /**
         * Stop the verticle and clean up resources
         * 
         * @throws Exception if an error occurs during shutdown
         */
        @Override
        public void stop() throws Exception {
                running.set(false);
                if (consumer != null)
                        consumer.close();
                if (producer != null)
                        producer.close();
                logger.info(Colors.RED + "[ FLOWAGGREGATOR VERTICLE ] FlowAggregatorVerticle stopped." + Colors.RESET);
        }

        /**
         * Process a single JSON record from Kafka, update or create flow
         * 
         * @param jsonStr JSON string representing a network packet
         */
        private void processRecord(JsonObject json) throws Exception {
                // Parse the raw packet data
                String rawPacketBase64 = json.getString("rawPacket");
                if (rawPacketBase64 == null) {
                        logger.warn("[ FLOWAGGREGATOR VERTICLE ] No rawPacket field in JSON.");
                        return;
                }

                byte[] rawData = Base64.getDecoder().decode(rawPacketBase64);
                Packet packet = EthernetPacket.newPacket(rawData, 0, rawData.length);
                logger.debug("[ FLOWAGGREGATOR VERTICLE ] Parsed packet: " + packet);

                if (!packet.contains(IpPacket.class))
                        return; // not IP, ignore packet

                IpPacket ipPacket = packet.get(IpPacket.class);

                String srcIp = getPacketSrcIp(ipPacket);
                String dstIp = getPacketDstIp(ipPacket);
                String protocol = getPacketProtocol(ipPacket);

                Long ts = json.getLong("timestamp", System.currentTimeMillis());
                Long bytes = json.getLong("length", (long) rawData.length);

                Ports ports = getPacketPorts(ipPacket);
                Integer srcPort = ports.src;
                Integer dstPort = ports.dst;

                // Check for TCP FIN/RST to end flow early
                AtomicBoolean flowEnded = new AtomicBoolean(false);

                if (ipPacket.contains(TcpPacket.class)) {
                        TcpPacket tcp = ipPacket.get(TcpPacket.class);
                        TcpPacket.TcpHeader tcpHeader = tcp.getHeader();

                        if (tcpHeader.getFin() || tcpHeader.getRst()) {
                                flowEnded.set(true);
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ] TCP termination detected (FIN/RST). Will flush early.");
                        }
                }

                logger.debug(String.format(
                                "[ FLOWAGGREGATOR VERTICLE ] Processing packet: %s:%d -> %s:%d protocol=%s bytes=%d ts=%d",
                                srcIp, srcPort == null ? 0 : srcPort,
                                dstIp, dstPort == null ? 0 : dstPort,
                                protocol, bytes, ts, flowEnded));

                // === Bilatéral : rendre la clé canonique ===
                String key = buildBilateralFlowKey(srcIp, srcPort, dstIp, dstPort, protocol);

                flows.compute(key, (k, f) -> {
                        if (f == null) {
                                f = new Flow(k, srcIp, dstIp, srcPort, dstPort, protocol, ts);
                        }
                        // update
                        f.lastSeen = ts;
                        f.firstSeen = Math.min(f.firstSeen, ts);
                        f.packetCount++;
                        f.bytes += bytes != null ? bytes : 0;
                        logger.debug("[ FLOWAGGREGATOR VERTICLE ] Flow updated: \n\t" + f.key
                                        + "\n\t firstSeen=" + f.firstSeen
                                        + " lastSeen=" + f.lastSeen
                                        + " bytes=" + f.bytes
                                        + " packets=" + f.packetCount);
                        // if flow ended (TCP FIN/RST), flush immediately
                        if (flowEnded.get()) {
                                publishFlow(f);
                                flows.remove(k);
                                logger.info("[ FLOWAGGREGATOR VERTICLE ] Flow flushed early due to FIN/RST: " + f.key);
                                return null; // remove from map
                        }

                        return f;
                });
        }

        /**
         * Extract source IP address from packet
         * 
         * @param packet the packet to extract the source IP from
         * @return the source IP address as a String, or null if not found
         */
        String getPacketSrcIp(Packet packet) {
                if (packet.contains(IpPacket.class)) {
                        IpPacket ipPacket = packet.get(IpPacket.class);
                        return ipPacket.getHeader().getSrcAddr().getHostAddress();
                }
                return null;
        }

        /**
         * Extract destination IP address from packet
         * 
         * @param packet the packet to extract the destination IP from
         * @return the destination IP address as a String, or null if not found
         */
        String getPacketDstIp(Packet packet) {
                if (packet.contains(IpPacket.class)) {
                        IpPacket ipPacket = packet.get(IpPacket.class);
                        return ipPacket.getHeader().getDstAddr().getHostAddress();
                }
                return null;
        }

        /**
         * Extract protocol from packet
         * 
         * @param packet the packet to extract the protocol from
         * @return the protocol as a String, or null if not found
         */
        String getPacketProtocol(Packet packet) {
                if (packet.contains(IpPacket.class)) {
                        IpPacket ipPacket = packet.get(IpPacket.class);
                        return ipPacket.getHeader().getProtocol().name();
                }
                return null;
        }

        /**
         * Helper class to hold source and destination ports
         */
        record Ports(Integer src, Integer dst) {
        }

        /**
         * Helper class to hold source and destination ports
         * 
         * @param ipPacket the IP packet to extract ports from
         * @return a Ports object containing source and destination ports, or null if
         *         not applicable
         */
        private Ports getPacketPorts(Packet ipPacket) {
                if (ipPacket.getPayload() instanceof TcpPacket tcp) {
                        return new Ports(tcp.getHeader().getSrcPort().valueAsInt(),
                                        tcp.getHeader().getDstPort().valueAsInt());
                } else if (ipPacket.getPayload() instanceof UdpPacket udp) {
                        return new Ports(udp.getHeader().getSrcPort().valueAsInt(),
                                        udp.getHeader().getDstPort().valueAsInt());
                }
                return new Ports(null, null);
        }

        /**
         * Build a canonical flow key from 5-tuple
         * 
         * @param srcIp    the source IP address
         * @param dstIp    the destination IP address
         * @param srcPort  the source port
         * @param dstPort  the destination port
         * @param protocol the protocol used
         * @return the constructed flow key
         */
        private String buildFlowKey(String srcIp, String dstIp, Integer srcPort, Integer dstPort, String protocol) {
                return String.format("%s|%s|%d|%d|%s", srcIp, dstIp, srcPort == null ? 0 : srcPort,
                                dstPort == null ? 0 : dstPort,
                                protocol == null ? "UNKNOWN" : protocol);
        }

        /**
         * Build a bilateral flow key from 5-tuple
         * 
         * @param srcIp    the source IP address
         * @param dstIp    the destination IP address
         * @param srcPort  the source port
         * @param dstPort  the destination port
         * @param protocol the protocol used
         * @return the constructed bilateral flow key
         */
        private String buildBilateralFlowKey(String srcIp, Integer srcPort,
                        String dstIp, Integer dstPort,
                        String protocol) {
                String a = srcIp + ":" + (srcPort == null ? 0 : srcPort);
                String b = dstIp + ":" + (dstPort == null ? 0 : dstPort);

                // On met toujours la "plus petite" paire en premier
                if (a.compareTo(b) <= 0) {
                        return a + "-" + b + "-" + protocol;
                } else {
                        return b + "-" + a + "-" + protocol;
                }
        }

        /**
         * Publish flow to Kafka topic as JSON
         * 
         * @param f Flow to publish
         */
        private void publishFlow(Flow f) {
                JsonObject jo = new JsonObject();
                jo.put("firstSeen", f.firstSeen);
                jo.put("lastSeen", f.lastSeen);
                jo.put("srcIp", f.srcIp);
                jo.put("dstIp", f.dstIp);
                jo.put("srcPort", f.srcPort);
                jo.put("dstPort", f.dstPort);
                jo.put("protocol", f.protocol);
                jo.put("bytes", f.bytes);
                jo.put("packetCount", f.packetCount);
                jo.put("durationMs", f.lastSeen - f.firstSeen);
                jo.put("flowKey", f.key);

                String value = jo.encode();

                ProducerRecord<String, String> rec = new ProducerRecord<>(OUT_TOPIC, f.key, value);
                producer.send(rec, (metadata, ex) -> {
                        if (ex != null) {
                                ex.printStackTrace();
                        } else {
                                logger.debug(String.format(
                                                "[ FLOWAGGREGATOR VERTICLE ] Published flow: key=%s bytes=%d packets=%d durationMs=%d",
                                                f.key, f.bytes, f.packetCount, (f.lastSeen - f.firstSeen)));
                        }
                });
        }

        /**
         * Internal Flow class to hold flow state
         * Represents a network flow with its attributes and timestamps.
         */
        private static class Flow {
                final String key;
                final String srcIp;
                final String dstIp;
                final Integer srcPort;
                final Integer dstPort;
                final String protocol;
                long firstSeen;
                long lastSeen;
                long bytes = 0;
                long packetCount = 0;

                Flow(String key, String srcIp, String dstIp, Integer srcPort, Integer dstPort, String protocol,
                                long ts) {
                        this.key = key;
                        this.srcIp = srcIp;
                        this.dstIp = dstIp;
                        this.srcPort = srcPort;
                        this.dstPort = dstPort;
                        this.protocol = protocol;
                        this.firstSeen = ts;
                        this.lastSeen = ts;
                        this.packetCount = 0;
                        this.bytes = 0;
                }
        }

}
