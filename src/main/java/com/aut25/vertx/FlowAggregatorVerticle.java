package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.IpPacket;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.IpV6Packet;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.TcpPacket;
import org.pcap4j.packet.UdpPacket;
import org.pcap4j.packet.factory.PacketFactories;
import org.pcap4j.packet.namednumber.DataLinkType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.sleep;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public class FlowAggregatorVerticle extends AbstractVerticle {

        private static final Logger logger = LoggerFactory.getLogger(FlowAggregatorVerticle.class);

        private static final String BOOTSTRAP_SERVERS = "localhost:9092";
        private static final String IN_TOPIC = "network-data";
        private static final String OUT_TOPIC = "network-flows";
        private static final String GROUP_ID = "flow-aggregator-group";

        private static final long FLOW_INACTIVITY_TIMEOUT_MS_TCP = 5_000; // 5 seconds
        private static final long FLOW_MAX_AGE_MS_TCP = 300_000; // 5 minutes
        private static final long FLOW_INACTIVITY_TIMEOUT_MS_UDP = 30_000; // 30 seconds
        private static final long FLOW_MAX_AGE_MS_UDP = 120_000; // 2 minutes

        private static final long FLOW_CLEAN_PERIOD_MS = 1_000;

        private KafkaConsumer<String, String> consumer;
        private KafkaProducer<String, String> producer;

        private final Map<String, Flow> flows = new ConcurrentHashMap<>();
        private final AtomicBoolean running = new AtomicBoolean(true);
        private long notIpPacketCount = 0;
        private long flushedEarlyCount = 0;
        private long ipv4PacketCount = 0;
        private long ipv6PacketCount = 0;
        private long arpPacketCount = 0;
        private long vlanPacketCount = 0;
        private long unknownPacketCount = 0;
        private long nonEthernetCount = 0;

        private final Map<String, NdpiFlowWrapper> ndpiFlows = new ConcurrentHashMap<>();

        // Start ndpi
        private final NDPIWrapper ndpi = new NDPIWrapper();

        @Override
        public void start() throws Exception {
                logger.info(Colors.GREEN + "[ FLOWAGGREGATOR VERTICLE ]       Starting FlowAggregatorVerticle..."
                                + Colors.RESET);
                // Initialize nDPI
                try {
                        ndpi.init();
                        logger.info(Colors.GREEN
                                        + "[ FLOWAGGREGATOR VERTICLE ]       nDPI initialized successfully in FlowAggregatorVerticle."
                                        + Colors.RESET);
                } catch (Exception e) {
                        logger.error(Colors.RED + "[ FLOWAGGREGATOR VERTICLE ]       Failed to initialize nDPI: "
                                        + e.getMessage() + Colors.RESET);
                        return;
                }

                // Kafka consumer config
                Map<String, String> consumerConfig = new HashMap<>();
                consumerConfig.put("bootstrap.servers", BOOTSTRAP_SERVERS);
                consumerConfig.put("group.id", GROUP_ID);
                consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                consumerConfig.put("auto.offset.reset", "earliest");
                consumerConfig.put("enable.auto.commit", "false");

                consumer = KafkaConsumer.create(vertx, consumerConfig);

                // Kafka producer config
                Map<String, String> producerConfig = new HashMap<>();
                producerConfig.put("bootstrap.servers", BOOTSTRAP_SERVERS);
                producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                producerConfig.put("acks", "1");

                producer = KafkaProducer.create(vertx, producerConfig);

                // Subscribe to input topic
                consumer.subscribe(IN_TOPIC, ar -> {
                        if (ar.succeeded()) {
                                logger.info(Colors.CYAN + "[ FLOWAGGREGATOR VERTICLE ]       Subscribed to topic {}",
                                                IN_TOPIC + Colors.RESET);
                        } else {
                                logger.error("[ FLOWAGGREGATOR VERTICLE ]       Failed to subscribe: {}",
                                                ar.cause().getMessage());
                        }
                });

                // Handler for incoming Kafka messages
                consumer.handler(record -> {
                        if (!running.get())
                                return;

                        String value = record.value();
                        if (value == null || value.isEmpty() || value.equals("reset")) {
                                // Display packet info
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Empty or Reset Packet details: {}",
                                                record);

                                return;
                        }

                        try {
                                JsonObject json = new JsonObject(value);
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Processing record: {}",
                                                json.encodePrettily());

                                processRecord(json);

                        } catch (Exception e) {
                                logger.error("[ FLOWAGGREGATOR VERTICLE ]       Error processing record: {}",
                                                e.getMessage());
                        }

                        // Commit offsets manually
                        consumer.commit(ar -> {
                                if (ar.failed()) {
                                        if (ar.cause().getMessage() != null) {
                                                logger.error(
                                                                "[ FLOWAGGREGATOR VERTICLE ]       Commit failed: "
                                                                                + ar.cause().getMessage());
                                        }
                                }
                        });
                });

                // Periodic cleanup task to flush expired flows
                vertx.setPeriodic(FLOW_CLEAN_PERIOD_MS, id -> {
                        if (!running.get())
                                return;

                        long now = System.currentTimeMillis();
                        List<Flow> toFlush = new ArrayList<>();

                        for (Flow f : flows.values()) {
                                long inactivityTimeout;
                                long maxAge;
                                if ("TCP".equals(f.protocol)) {
                                        inactivityTimeout = FLOW_INACTIVITY_TIMEOUT_MS_TCP;
                                        maxAge = FLOW_MAX_AGE_MS_TCP;
                                } else if ("UDP".equals(f.protocol)) {
                                        inactivityTimeout = FLOW_INACTIVITY_TIMEOUT_MS_UDP;
                                        maxAge = FLOW_MAX_AGE_MS_UDP;
                                } else {
                                        inactivityTimeout = 10_000;
                                        maxAge = 120_000;
                                }

                                if (now - f.lastSeen >= inactivityTimeout || now - f.firstSeen >= maxAge) {
                                        if (flows.remove(f.key) != null) {
                                                toFlush.add(f);
                                        }
                                        // Also remove from ndpiFlows
                                        ndpiFlows.remove(f.key);
                                }
                                f.appProtocol = getNDPIProcol(f);
                        }

                        long tcpCountToFlush = toFlush.stream().filter(f -> "TCP".equals(f.protocol)).count();
                        long udpCountToFlush = toFlush.stream().filter(f -> "UDP".equals(f.protocol)).count();
                        long tcpCount = flows.values().stream().filter(f -> "TCP".equals(f.protocol)).count();
                        long udpCount = flows.values().stream().filter(f -> "UDP".equals(f.protocol)).count();

                        for (Flow f : toFlush) {
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ] Flushing flow: key={} bytes={} packets={} durationMs={}",
                                                f.key, f.bytes, f.packetCount, (f.lastSeen - f.firstSeen));

                                // nDPI analysis on flow packets if not already detected
                                // before publishing
                                f.appProtocol = getNDPIProcol(f);
                                f.riskLevel = getNDPIFlowRisk(f);
                                f.riskMask = getNDPIFlowRiskMask(f);
                                f.riskLabel = getNDPIFlowRiskLabel(f);
                                f.riskSeverity = getNDPIFlowRiskSeverity(f);

                                // Publish the flow with appProtocol, riskLevel, and riskLabel set
                                publishFlow(f);

                                // Free flow in nDPI
                                // if (f.ndpiFlowPtr != 0) {
                                // try {
                                // ndpi.cleanup(f.ndpiFlowPtr);
                                // logger.debug("[ FLOWAGGREGATOR VERTICLE ] Cleaned up nDPI flow for key={}",
                                // f.key);
                                // } catch (Exception e) {
                                // logger.warn(
                                // "[ FLOWAGGREGATOR VERTICLE ] Failed to clean up nDPI flow for key={} : {}",
                                // f.key, e.getMessage());
                                // }
                                // }

                        }

                        Map<String, Long> protocolCounts = toFlush.stream()
                                        .collect(Collectors.groupingBy(f -> f.protocol, Collectors.counting()));

                        logger.info("[ FLOWAGGREGATOR VERTICLE ]       Flushed {} flows (TCP : {} | UDP : {})",
                                        toFlush.size(),
                                        tcpCountToFlush,
                                        udpCountToFlush);

                        protocolCounts.forEach((protocol, count) -> {
                                logger.info("[ FLOWAGGREGATOR VERTICLE ]       Flushed {} flows for protocol: {}",
                                                count, protocol);
                        });

                        logger.info("[ FLOWAGGREGATOR VERTICLE ]       Flushed early (FIN/RST) {} flows",
                                        flushedEarlyCount);
                        logger.info("[ FLOWAGGREGATOR VERTICLE ]       >> Active flows: {} (TCP : {} | UDP : {})",
                                        flows.size(),
                                        tcpCount,
                                        udpCount);
                        logger.info("[ FLOWAGGREGATOR VERTICLE ]       >> Not IP packets processed: {}",
                                        notIpPacketCount);
                        logger.info("---------------------------------------------------------------------------------------------");

                });

                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       FlowAggregatorVerticle started.");
        }

        /**
         * Get nDPI detected protocol for a flow
         * 
         * @param f Flow to analyze
         * @return detected protocol as String, or "UNKNOWN" if not identified
         */
        private String getNDPIProcol(Flow f) {
                if (f.ndpiFlowPtr != 0 && !f.getPacketsByte().isEmpty()) {
                        String proto = "UNKNOWN";
                        for (byte[] pkt : f.getPacketsByte()) {
                                try {
                                        proto = ndpi.analyzePacket(pkt, f.lastSeen,
                                                        f.ndpiFlowPtr);

                                } catch (Exception e) {
                                        logger.warn("Failed to analyze packet for flow {}: {}", f.key,
                                                        e.getMessage());
                                }
                        }
                        return proto;
                }
                return "UNKNOWN";
        }

        /**
         * Get nDPI risk level for a flow
         * 
         * @param f Flow to analyze
         * @return risk level as int, or 0 if not identified
         */
        private int getNDPIFlowRisk(Flow f) {
                if (f.ndpiFlowPtr != 0) {
                        try {
                                int riskScore = ndpi.getFlowRiskScore(f.ndpiFlowPtr);
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Flow {} has nDPI riskScore={}",
                                                f.key, riskScore);
                                return riskScore;
                        } catch (Exception e) {
                                logger.warn("Failed to get nDPI risk for flow {}: {}", f.key,
                                                e.getMessage());
                        }
                }
                return 0;
        }

        /**
         * Get nDPI risk mask for a flow
         * 
         * @param f Flow to analyze
         * @return risk mask as int, or 0 if not identified
         */
        private int getNDPIFlowRiskMask(Flow f) {
                if (f.ndpiFlowPtr != 0) {
                        try {
                                int riskMask = ndpi.getFlowRiskMask(f.ndpiFlowPtr);
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Flow {} has nDPI riskMask={}",
                                                f.key, riskMask);
                                return riskMask;
                        } catch (Exception e) {
                                logger.warn("Failed to get nDPI risk mask for flow {}: {}", f.key,
                                                e.getMessage());
                        }
                }
                return 0;
        }

        /**
         * Get nDPI risk label for a flow
         * 
         * @param f Flow to analyze
         * @return risk label as String, or "UNKNOWN" if not identified
         */
        private String getNDPIFlowRiskLabel(Flow f) {
                if (f.ndpiFlowPtr != 0) {
                        try {
                                String riskLabel = ndpi.getFlowRiskLabel(f.ndpiFlowPtr);
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Flow {} has nDPI riskLabel={}",
                                                f.key, riskLabel);
                                return riskLabel;
                        } catch (Exception e) {
                                logger.warn("Failed to get nDPI risk label for flow {}: {}", f.key,
                                                e.getMessage());
                        }
                }
                return "UNKNOWN";
        }

        /**
         * Get nDPI risk severity for a flow
         * 
         * @param f Flow to analyze
         * @return risk severity as String, or "UNKNOWN" if not identified
         */
        private String getNDPIFlowRiskSeverity(Flow f) {
                if (f.ndpiFlowPtr != 0) {
                        try {
                                String riskSeverity = ndpi.getFlowRiskSeverity(f.ndpiFlowPtr);
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Flow {} has nDPI riskSeverity={}",
                                                f.key, riskSeverity);
                                return riskSeverity;
                        } catch (Exception e) {
                                logger.warn("Failed to get nDPI risk severity for flow {}: {}", f.key,
                                                e.getMessage());
                        }
                }
                return "UNKNOWN";
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
                logger.info(Colors.RED + "[ FLOWAGGREGATOR VERTICLE ]       FlowAggregatorVerticle stopped!"
                                + Colors.RESET);
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
                        logger.warn("[ FLOWAGGREGATOR VERTICLE ]       No rawPacket field in JSON.");
                        return;
                }

                byte[] rawData = Base64.getDecoder().decode(rawPacketBase64);
                Packet packet;
                try {
                        packet = EthernetPacket.newPacket(rawData, 0, rawData.length);
                } catch (Throwable t) {
                        logger.error("[ FLOWAGGREGATOR ] Cannot create Packet from raw bytes: {}",
                                        t.getMessage());
                        return; // skip this record
                }

                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Parsed packet: " + packet);
                EthernetPacket eth = packet.get(EthernetPacket.class);
                if (eth == null) {
                        logger.warn("[FLOWAGGREGATOR] Not an Ethernet packet.");
                        nonEthernetCount++;
                        return;
                }

                EthernetPacket.EthernetHeader ethHeader = eth.getHeader();
                int etherType = ethHeader.getType().value() & 0xFFFF;

                switch (etherType) {
                        case 0x0800: // IPv4
                                handleIPv4(packet, json, rawData);
                                break;

                        case 0x86DD: // IPv6
                                handleIPv6(packet, json, rawData);
                                break;

                        case 0x0806: // ARP
                                handleArp(packet);
                                break;

                        case 0x8100: // VLAN
                                handleVlanEncapsulated(packet, json, rawData);
                                break;

                        default:
                                logger.debug("[FLOWAGGREGATOR] Unsupported EtherType: 0x{}",
                                                Integer.toHexString(etherType));
                                unknownPacketCount++;
                }

        }

        private void handleIPv4(Packet packet, JsonObject json, byte[] rawData) {
                IpV4Packet ipv4 = packet.get(IpV4Packet.class);
                if (ipv4 == null) {
                        logger.warn("[ FLOWAGGREGATOR VERTICLE ]       Not an IPv4 packet, skipping.");
                        notIpPacketCount++;
                        return;
                }
                ipv4PacketCount++;
                processIpPacket(packet, json, rawData);
        }

        private void handleIPv6(Packet packet, JsonObject json, byte[] rawData) {
                IpV6Packet ipv6 = packet.get(IpV6Packet.class);
                if (ipv6 == null) {
                        logger.warn("[ FLOWAGGREGATOR VERTICLE ]       Not an IPv6 packet, skipping.");
                        notIpPacketCount++;
                        return;
                }
                ipv6PacketCount++;
                processIpPacket(packet, json, rawData);
        }

        private void processIpPacket(Packet packet, JsonObject json, byte[] rawData) {
                IpPacket ipPacket = packet.get(IpPacket.class);
                if (ipPacket == null) {
                        logger.warn("[ FLOWAGGREGATOR VERTICLE ]       Not an IP packet, skipping.");
                        notIpPacketCount++;
                        // Display packet info
                        return;
                }

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
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       TCP termination detected (FIN/RST). Will flush early.");
                        }
                }

                logger.debug(String.format(
                                "[ FLOWAGGREGATOR VERTICLE ]       Processing packet: %s:%d -> %s:%d protocol=%s bytes=%d ts=%d",
                                srcIp, srcPort == null ? 0 : srcPort,
                                dstIp, dstPort == null ? 0 : dstPort,
                                protocol, bytes, ts, flowEnded));

                // Build bilateral flow key
                String key = buildBilateralFlowKey(srcIp, srcPort, dstIp, dstPort, protocol);

                NdpiFlowWrapper ndpiFlow = ndpiFlows.computeIfAbsent(key, k -> {
                        // Create new nDPI flow
                        long flowPtr = ndpi.createFlow();
                        return new NdpiFlowWrapper(flowPtr);
                });

                ndpiFlow.lastSeen = ts;

                // Send packet to nDPI for analysis
                try {
                        IpPacket ip = packet.get(IpPacket.class); // IpPacket
                        byte[] payload = ip.getRawData();
                        logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Payload : {} " + payload);

                        String ndpiProtocol = ndpi.analyzePacket(payload, ts, ndpiFlow.ndpiFlowPtr); // analyse

                        logger.debug("[ FLOWAGGREGATOR VERTICLE ] Sent to nDPI {} bytes for flow {}",
                                        payload.length, key);

                        if (!"UNKNOWN".equalsIgnoreCase(ndpiProtocol)) {
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ] Flow {} analyzed with nDPI: {}",
                                                key, ndpiProtocol);
                        }

                        ndpiFlow.detectedProtocol = ndpiProtocol;

                } catch (Exception e) {
                        logger.warn("[ FLOWAGGREGATOR VERTICLE ] Could not analyze flow with nDPI: {}",
                                        e.getMessage());
                }

                flows.compute(key, (k, f) -> {
                        if (f == null) {
                                f = new Flow(k, srcIp, dstIp, srcPort, dstPort, protocol, ts);
                                f.ndpiFlowPtr = ndpiFlow.ndpiFlowPtr;
                        }
                        // update
                        f.addPacket(packet, ts);
                        f.lastSeen = Math.max(f.lastSeen, ts);
                        f.firstSeen = Math.min(f.firstSeen, ts);
                        f.packetCount++;
                        f.bytes += bytes != null ? bytes : 0;

                        logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Flow updated: {} firstSeen={} lastSeen={} bytes={} packets={}",
                                        f.key, f.firstSeen, f.lastSeen, f.bytes, f.packetCount);

                        // return f normally; do not flush inside compute
                        return f;
                });

                // after compute, check if the flow should be flushed
                if (flowEnded.get()) {
                        Flow endedFlow = flows.remove(key);
                        if (endedFlow != null) {
                                publishFlow(endedFlow);
                                flushedEarlyCount++;
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Flow flushed early due to FIN/RST: "
                                                + endedFlow.key);
                        }
                }
        }

        private void handleArp(Packet packet) {
                arpPacketCount++;
                notIpPacketCount++;
                EthernetPacket eth = packet.get(EthernetPacket.class);
                if (eth != null) {
                        // Extract ARP header details
                        String srcIp = eth.getHeader().getSrcAddr().toString();
                        String dstIp = eth.getHeader().getDstAddr().toString();
                        String protocol = "ARP";

                        // ARP packets don't have ports, so use 0 as placeholders
                        Integer srcPort = 0;
                        Integer dstPort = 0;

                        // Build a flow key for ARP
                        String key = buildFlowKey(srcIp, dstIp, srcPort, dstPort, protocol);

                        // Create flow and publish immediately
                        Flow arpFlow = new Flow(key, srcIp, dstIp, srcPort, dstPort, protocol,
                                        System.currentTimeMillis());
                        arpFlow.addPacket(packet, System.currentTimeMillis());
                        arpFlow.packetCount = 1;
                        arpFlow.bytes = (long) eth.length();
                        arpFlow.lastSeen = System.currentTimeMillis();
                        arpFlow.firstSeen = arpFlow.lastSeen;
                        publishFlow(arpFlow);

                        logger.info("[ FLOWAGGREGATOR VERTICLE ]       ARP flow created/updated: key={} srcIp={} dstIp={} bytes={}",
                                        key, srcIp, dstIp, eth.length());

                } else {
                        logger.info("[ FLOWAGGREGATOR VERTICLE ]       ARP packet encountered: packet={}", packet);
                }
        }

        private void handleVlanEncapsulated(Packet packet, JsonObject json, byte[] rawData) {
                vlanPacketCount++;
                // Extract the encapsulated Ethernet frame
                Packet payload = packet.getPayload();
                if (payload != null && payload instanceof EthernetPacket) {
                        EthernetPacket innerEth = (EthernetPacket) payload;
                        int innerEtherType = innerEth.getHeader().getType().value() & 0xFFFF;

                        switch (innerEtherType) {
                                case 0x0800: // IPv4
                                        handleIPv4(innerEth, json, rawData);
                                        break;

                                case 0x86DD: // IPv6
                                        handleIPv6(innerEth, json, rawData);
                                        break;

                                case 0x0806: // ARP
                                        handleArp(innerEth);
                                        break;

                                default:
                                        logger.error("[ FLOWAGGREGATOR VERTICLE ]       VLAN with unsupported inner EtherType: 0x{}",
                                                        Integer.toHexString(innerEtherType));
                                        unknownPacketCount++;
                        }
                } else {
                        logger.error("[ FLOWAGGREGATOR VERTICLE ]       VLAN packet does not contain an inner Ethernet frame.");
                        notIpPacketCount++;
                }
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
                f.calculateStats();
                JsonObject jo = f.getJsonObject();
                String value = jo.encode();

                logger.info("[ FLOWAGGREGATOR VERTICLE ]       Published flow: key={} appProtocol={} riskLevel={} riskLabel={} riskSeverity={} bytes={} packets={} durationMs={}",
                                f.key, f.appProtocol, f.riskLevel, f.riskLabel, f.riskSeverity, f.bytes,
                                f.packetCount,
                                (f.lastSeen - f.firstSeen));

                KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(OUT_TOPIC, f.key, value);

                producer.write(record, ar -> {
                        if (ar.failed()) {
                                logger.error("[ FLOWAGGREGATOR VERTICLE ]       Failed to publish flow {}: {}", f.key,
                                                ar.cause().getMessage());
                        } else {
                                // f.display();
                        }
                });

        }

}
