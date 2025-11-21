package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;

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

import com.aut25.vertx.services.DnsService;
import com.aut25.vertx.services.GeoIPService;
import com.aut25.vertx.services.WhoisService;
import com.aut25.vertx.utils.Colors;
import com.aut25.vertx.utils.Flow;
import com.aut25.vertx.utils.NDPIWrapper;
import com.aut25.vertx.utils.NdpiFlowWrapper;

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

        private long FLOW_INACTIVITY_TIMEOUT_MS_TCP = 5_000; // 5 seconds
        private long FLOW_MAX_AGE_MS_TCP = 300_000; // 5 minutes
        private long FLOW_INACTIVITY_TIMEOUT_MS_UDP = 5_000; // 5 seconds
        private long FLOW_MAX_AGE_MS_UDP = 300_000; // 5 minutes
        private long FLOW_INACTIVITY_TIMEOUT_MS_OTHER = 5_000; // 5 seconds
        private long FLOW_MAX_AGE_MS_OTHER = 300_000; // 5 minutes

        private static final long FLOW_CLEAN_PERIOD_MS = 100;

        private KafkaConsumer<String, String> consumer;
        private KafkaProducer<String, String> producer;

        private final Map<String, Flow> flows = new ConcurrentHashMap<>();
        private List<Flow> toFlush = new ArrayList<>();
        private List<Flow> currentFlows = new ArrayList<>();
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

        // Enrichment services
        private GeoIPService geoIPService;
        private DnsService dnsService;
        private WhoisService whoisService;

        @Override
        public void start() throws Exception {
                logger.info(Colors.GREEN + "[ FLOWAGGREGATOR VERTICLE ]       Starting FlowAggregatorVerticle..."
                                + Colors.RESET);
                LocalMap<String, Object> map = vertx.sharedData().getLocalMap("config");
                JsonObject config = new JsonObject(map);
                if (config == null) {
                        logger.info(Colors.MAGENTA
                                        + "[ FLOWAGGREGATOR VERTICLE ]       Keeping default settings as no config found in shared map."
                                        + Colors.RESET);
                } else {
                        FLOW_INACTIVITY_TIMEOUT_MS_TCP = config.getLong("FLOW_INACTIVITY_TIMEOUT_MS_TCP",
                                        FLOW_INACTIVITY_TIMEOUT_MS_TCP);
                        FLOW_MAX_AGE_MS_TCP = config.getLong("FLOW_MAX_AGE_MS_TCP", FLOW_MAX_AGE_MS_TCP);
                        FLOW_INACTIVITY_TIMEOUT_MS_UDP = config.getLong("FLOW_INACTIVITY_TIMEOUT_MS_UDP",
                                        FLOW_INACTIVITY_TIMEOUT_MS_UDP);
                        FLOW_MAX_AGE_MS_UDP = config.getLong("FLOW_MAX_AGE_MS_UDP", FLOW_MAX_AGE_MS_UDP);
                        FLOW_INACTIVITY_TIMEOUT_MS_OTHER = config.getLong("FLOW_INACTIVITY_TIMEOUT_MS_OTHER",
                                        FLOW_INACTIVITY_TIMEOUT_MS_OTHER);
                        FLOW_MAX_AGE_MS_OTHER = config.getLong("FLOW_MAX_AGE_MS_OTHER", FLOW_MAX_AGE_MS_OTHER);
                        logger.info(Colors.GREEN + "[ FLOWAGGREGATOR VERTICLE ]       Loaded settings from shared map."
                                        + Colors.RESET);
                }
                logger.info(Colors.GREEN
                                + "[ FLOWAGGREGATOR VERTICLE ]       Flow timeouts: TCP inactivity={}ms, TCP max age={}ms, UDP inactivity={}ms, UDP max age={}ms, OTHER inactivity={}ms, OTHER max age={}ms"
                                + Colors.RESET,
                                FLOW_INACTIVITY_TIMEOUT_MS_TCP, FLOW_MAX_AGE_MS_TCP,
                                FLOW_INACTIVITY_TIMEOUT_MS_UDP, FLOW_MAX_AGE_MS_UDP,
                                FLOW_INACTIVITY_TIMEOUT_MS_OTHER, FLOW_MAX_AGE_MS_OTHER);
                // Init enrich services if needed
                geoIPService = new GeoIPService("src/main/resources/GeoLite2-City.mmdb",
                                "src/main/resources/GeoLite2-ASN.mmdb");
                dnsService = new DnsService();
                whoisService = new WhoisService();
                logger.info(Colors.GREEN + "[ FLOWAGGREGATOR VERTICLE ]       Enrichment services initialized."
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

                // Désérialisation classique
                consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

                // Toujours prendre le prochain message disponible
                consumerConfig.put("auto.offset.reset", "earliest");

                // Pour réduire la latence, commit manuel ou auto très fréquent
                consumerConfig.put("enable.auto.commit", "true");
                consumerConfig.put("auto.commit.interval.ms", "100"); // Valeur ultra faible

                // Latence minimale : récupérer le plus vite possible
                consumerConfig.put("max.poll.records", "1"); // 1 seul record = temps minimal
                consumerConfig.put("fetch.min.bytes", "1"); // Retourne dès qu’un octet est disponible
                consumerConfig.put("fetch.max.wait.ms", "0"); // Pas d’attente
                consumerConfig.put("fetch.max.bytes", "1048576"); // 1 MB suffit (réduit la copie mémoire)

                // Timeouts agressifs mais sûrs
                consumerConfig.put("session.timeout.ms", "10000"); // 10s
                consumerConfig.put("heartbeat.interval.ms", "3000"); // 3s
                consumerConfig.put("request.timeout.ms", "15000"); // 15s

                // Optimisation de la latence au niveau réseau
                consumerConfig.put("receive.buffer.bytes", "32768"); // 32 KB, bonne valeur pour réduire overhead
                consumerConfig.put("send.buffer.bytes", "32768");
                consumerConfig.put("fetch.buffer", "4096");

                // Pas de batching côté protocole
                consumerConfig.put("client.id", "low-latency-consumer");
                consumerConfig.put("max.partition.fetch.bytes", "1048576"); // 1MB max par fetch

                consumer = KafkaConsumer.create(vertx, consumerConfig);
                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Kafka consumer created : " + consumerConfig.toString());

                // Kafka producer config
                Map<String, String> producerConfig = new HashMap<>();
                producerConfig.put("bootstrap.servers", BOOTSTRAP_SERVERS);
                producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                producerConfig.put("acks", "1");

                producer = KafkaProducer.create(vertx, producerConfig);

                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Kafka producer created : " + producerConfig.toString());

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

                        Iterator<Flow> iterator = toFlush.iterator();
                        while (iterator.hasNext()) {
                                Flow f = iterator.next();
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ] Flushing flow: key={} bytes={} packets={} durationMs={}",
                                                f.key, f.bytes, f.packetCount, (f.lastSeen - f.firstSeen));

                                // nDPI analysis on flow packets if not already detected
                                // before publishing
                                f.appProtocol = getNDPIProcol(f);
                                f.riskLevel = getNDPIFlowRisk(f);
                                f.riskMask = getNDPIFlowRiskMask(f);
                                f.riskLabel = getNDPIFlowRiskLabel(f);
                                f.riskSeverity = getNDPIFlowRiskSeverity(f);

                                // Publish the flow with appProtocol, riskLevel, riskLabel, and reasonOfFlowEnd
                                // set
                                publishFlow(f, f.reasonOfFlowEnd);

                                // Remove the flow from the map after publishing
                                try {
                                        flows.entrySet().removeIf(entry -> entry.getKey().equals(f.key) &&
                                                        entry.getValue().firstSeen == f.firstSeen &&
                                                        entry.getValue().lastSeen == f.lastSeen);
                                } catch (Exception e) {
                                        logger.error("[ FLOWAGGREGATOR VERTICLE ]       Error removing flow {}: {}",
                                                        f.key, e.getMessage());
                                }
                                iterator.remove();
                        }

                });
        }

        /**
         * Publish a flow to the output Kafka topic
         * 
         * @param f Flow to publish
         */
        private void flushExpiredFlows(long referenceTimeMs) {
                if (!running.get())
                        return;

                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Reference time for flushing: {}",
                                referenceTimeMs);

                // Iterate safely over the entry set so we can remove while iterating
                Iterator<Map.Entry<String, Flow>> it = flows.entrySet().iterator();
                while (it.hasNext()) {
                        Map.Entry<String, Flow> entry = it.next();
                        Flow f = entry.getValue();

                        long inactivityTimeout;
                        long maxAge;
                        if ("TCP".equals(f.protocol)) {
                                inactivityTimeout = FLOW_INACTIVITY_TIMEOUT_MS_TCP;
                                maxAge = FLOW_MAX_AGE_MS_TCP;
                        } else if ("UDP".equals(f.protocol)) {
                                inactivityTimeout = FLOW_INACTIVITY_TIMEOUT_MS_UDP;
                                maxAge = FLOW_MAX_AGE_MS_UDP;
                        } else {
                                inactivityTimeout = FLOW_INACTIVITY_TIMEOUT_MS_OTHER;
                                maxAge = FLOW_MAX_AGE_MS_OTHER;
                        }

                        boolean inactive = (referenceTimeMs - f.lastSeen) >= inactivityTimeout;
                        boolean tooOld = (referenceTimeMs - f.firstSeen) >= maxAge;

                        if (inactive) {
                                f.reasonOfFlowEnd = "Inactivity Timeout";
                        } else if (tooOld) {
                                f.reasonOfFlowEnd = "Max Age Exceeded";
                        }

                        if (inactive || tooOld) {
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Checking flow: key={} inactivityTimeout={} maxAge={}",
                                                f.key, inactivityTimeout, maxAge);
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Inactivity duration: {} ms, Age duration: {} ms",
                                                (referenceTimeMs - f.lastSeen), (referenceTimeMs - f.firstSeen));
                                // remove via iterator to avoid ConcurrentModificationException on
                                // non-concurrent maps
                                it.remove();
                                // remove from ndpiFlows as well
                                ndpiFlows.entrySet().removeIf(ndpiEntry -> ndpiEntry.getKey().equals(entry.getKey()) &&
                                                ndpiEntry.getValue().lastSeen == f.lastSeen);
                                toFlush.add(f);

                        } else {
                                // Collect all ongoing flows that are not finished
                                List<Flow> ongoingFlows = flows.values().stream()
                                                .filter(flow -> !toFlush.contains(flow))
                                                .collect(Collectors.toList());

                                // Create a JSON array to represent the ongoing flows
                                JsonObject ongoingFlowsJson = new JsonObject()
                                                .put("flows", ongoingFlows.stream()
                                                                .map(Flow::getJsonObject)
                                                                .collect(Collectors.toList()));

                                // Check if the current flows are different from the last published flows
                                if (!ongoingFlowsJson.equals(new JsonObject().put("flows", currentFlows.stream()
                                                .map(Flow::getJsonObject)
                                                .collect(Collectors.toList())))) {
                                        // Update the current flows
                                        currentFlows = new ArrayList<>(ongoingFlows);

                                        // Send the ongoing flows to the event bus
                                        vertx.eventBus().publish("currentFlows.data", ongoingFlowsJson);
                                }
                        }
                }
        }

        private void flushRemainingFlows() {
                logger.info(Colors.CYAN
                                + "[ FLOWAGGREGATOR VERTICLE ]       Flushing remaining flows (end of pcap)..."
                                + Colors.RESET);
                toFlush = new ArrayList<>(flows.values());

                Iterator<Flow> iterator = toFlush.iterator();
                while (iterator.hasNext()) {
                        Flow f = iterator.next();
                        logger.debug("[ FLOWAGGREGATOR VERTICLE ] Flushing remaining flow: key={} bytes={} packets={} durationMs={}",
                                        f.key, f.bytes, f.packetCount, (f.lastSeen - f.firstSeen));

                        // nDPI analysis on flow packets if not already detected
                        // before publishing
                        f.appProtocol = getNDPIProcol(f);
                        f.riskLevel = getNDPIFlowRisk(f);
                        f.riskMask = getNDPIFlowRiskMask(f);
                        f.riskLabel = getNDPIFlowRiskLabel(f);
                        f.riskSeverity = getNDPIFlowRiskSeverity(f);

                        // Publish the flow with appProtocol, riskLevel, riskLabel, and reasonOfFlowEnd
                        // set
                        publishFlow(f, "PCAP_DONE");
                        // Remove the flow from the map after publishing
                        try {
                                flows.entrySet().removeIf(entry -> entry.getKey().equals(f.key) &&
                                                entry.getValue().lastSeen == f.lastSeen &&
                                                entry.getValue().firstSeen == f.firstSeen);
                                ndpiFlows.entrySet().removeIf(entry -> entry.getKey().equals(f.key) &&
                                                entry.getValue().lastSeen == f.lastSeen);
                        } catch (Exception e) {
                                logger.error("[ FLOWAGGREGATOR VERTICLE ]       Error removing flow {}: {}",
                                                f.key, e.getMessage());
                        }
                        iterator.remove();

                        List<Flow> ongoingFlows = flows.values().stream()
                                        .filter(flow -> !toFlush.contains(flow))
                                        .collect(Collectors.toList());

                        // Create a JSON array to represent the ongoing flows
                        JsonObject ongoingFlowsJson = new JsonObject()
                                        .put("flows", ongoingFlows.stream()
                                                        .map(Flow::getJsonObject)
                                                        .collect(Collectors.toList()));

                        // Check if the current flows are different from the last published flows
                        if (!ongoingFlowsJson.equals(new JsonObject().put("flows", currentFlows.stream()
                                        .map(Flow::getJsonObject)
                                        .collect(Collectors.toList())))) {
                                // Update the current flows
                                currentFlows = new ArrayList<>(ongoingFlows);

                                // Send the ongoing flows to the event bus
                                vertx.eventBus().publish("currentFlows.data", ongoingFlowsJson);
                        }
                }

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
                if (json.toString().contains("PCAP_DONE")) {
                        logger.info(Colors.CYAN
                                        + "[ FLOWAGGREGATOR VERTICLE ]       Received PCAP_DONE message. Finished processing pcap file."
                                        + Colors.RESET);
                        // flush any remaining flows
                        flushRemainingFlows();
                        return;
                }
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
                        logger.error("[ FLOWAGGREGATOR ] Cannot create Packet from raw bytes: {}. Raw data: {}",
                                        t.getMessage(), Base64.getEncoder().encodeToString(rawData));
                        // Publish malformed packet to the event bus
                        JsonObject malformedPacket = new JsonObject()
                                        .put("error", t.getMessage())
                                        .put("rawData", rawData);
                        vertx.eventBus().publish("malformedPackets.data", malformedPacket);
                        return; // skip this record
                }

                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Parsed packet: " + packet);
                EthernetPacket eth = packet.get(EthernetPacket.class);
                if (eth == null) {
                        logger.error("[FLOWAGGREGATOR] Not an Ethernet packet. Raw data: {}",
                                        Base64.getEncoder().encodeToString(rawData));
                        nonEthernetCount++;
                        // Publish malformed packet to the event bus
                        JsonObject malformedPacket = new JsonObject()
                                        .put("error", "Not an Ethernet packet")
                                        .put("rawData", packet.toString());
                        vertx.eventBus().publish("malformedPackets.data", malformedPacket);
                        return;
                }

                EthernetPacket.EthernetHeader ethHeader = eth.getHeader();
                int etherType = ethHeader.getType().value() & 0xFFFF;
                switch (etherType) {
                        case 0x0800: // IPv4
                                logger.debug("[FLOWAGGREGATOR] Ethernet Type: IPv4 (0x0800)");
                                handleIPv4(packet, json, rawData);
                                break;

                        case 0x86DD: // IPv6
                                logger.debug("[FLOWAGGREGATOR] Ethernet Type: IPv6 (0x86DD)");
                                handleIPv6(packet, json, rawData);
                                break;

                        case 0x0806: // ARP
                                logger.debug("[FLOWAGGREGATOR] Ethernet Type: ARP (0x0806)");
                                handleArp(packet, json);
                                break;

                        case 0x8100: // VLAN
                                logger.debug("[FLOWAGGREGATOR] Ethernet Type: VLAN (0x8100)");
                                handleVlanEncapsulated(packet, json, rawData);
                                break;

                        default:
                                logger.debug("[FLOWAGGREGATOR] Unsupported EtherType: 0x{}. Raw data: {}",
                                                Integer.toHexString(etherType),
                                                Base64.getEncoder().encodeToString(rawData));
                                JsonObject malformedPacket = new JsonObject()
                                                .put("error", "Unsupported EtherType: 0x"
                                                                + Integer.toHexString(etherType))
                                                .put("rawData", packet.toString());
                                vertx.eventBus().publish("malformedPackets.data", malformedPacket);
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
                flushExpiredFlows(ts);

                Long bytes = json.getLong("length", (long) rawData.length);

                Ports ports = getPacketPorts(ipPacket);
                Integer srcPort = ports.src;
                Integer dstPort = ports.dst;

                // Check for TCP FIN/RST to end flow early
                AtomicBoolean flowEnded = new AtomicBoolean(false);
                String endFlag = "";
                if (ipPacket.contains(TcpPacket.class)) {
                        TcpPacket tcp = ipPacket.get(TcpPacket.class);
                        TcpPacket.TcpHeader tcpHeader = tcp.getHeader();

                        if (tcpHeader.getFin()) {
                                flowEnded.set(true);
                                endFlag = "FIN";
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       TCP termination detected (FIN). Will flush early.");
                        } else if (tcpHeader.getRst()) {
                                flowEnded.set(true);
                                endFlag = "RST";
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       TCP termination detected (RST). Will flush early.");
                        }
                }

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

                        String ndpiProtocol = ndpi.analyzePacket(payload, ts, ndpiFlow.ndpiFlowPtr); // analyse

                        if (!"UNKNOWN".equalsIgnoreCase(ndpiProtocol)) {
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ] Flow {} analyzed with nDPI: {}",
                                                key, ndpiProtocol);
                        }

                        ndpiFlow.detectedProtocol = ndpiProtocol;

                } catch (Exception e) {
                        logger.warn("[ FLOWAGGREGATOR VERTICLE ] Could not analyze flow with nDPI: {}",
                                        e.getMessage());
                }
                // Update or create flow
                flows.compute(key, (k, f) -> {
                        if (f == null) {
                                f = new Flow(k, srcIp, dstIp, srcPort, dstPort, protocol, ts);
                                f.ndpiFlowPtr = ndpiFlow.ndpiFlowPtr;
                        }
                        // update
                        String packId = setPacketId(srcIp, dstIp, protocol, ts, packet);
                        f.addPacket(packet, ts, packId);
                        f.lastSeen = Math.max(f.lastSeen, ts);
                        f.firstSeen = Math.min(f.firstSeen, ts);
                        f.packetCount++;
                        f.bytes += bytes != null ? bytes : 0;
                        f.treatmentDelay.add(System.currentTimeMillis()
                                        - json.getLong("ingestedAt", 0L));

                        logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Flow updated: {} firstSeen={} lastSeen={} bytes={} packets={}",
                                        f.key, f.firstSeen, f.lastSeen, f.bytes, f.packetCount);

                        // return f normally; do not flush inside compute

                        return f;
                });

                // after compute, check if the flow should be flushed
                if (flowEnded.get()) {
                        Flow endedFlow = null;
                        Iterator<Map.Entry<String, Flow>> iterator = flows.entrySet().iterator();
                        while (iterator.hasNext()) {
                                Map.Entry<String, Flow> entry = iterator.next();
                                if (entry.getKey().equals(key) && entry.getValue().firstSeen == ts
                                                && entry.getValue().lastSeen == ts) {
                                        endedFlow = entry.getValue();
                                        iterator.remove();
                                        break;
                                }
                        }
                        ndpiFlows.entrySet().removeIf(entry -> entry.getKey().equals(key) &&
                                        entry.getValue().lastSeen == ts);

                        if (endedFlow != null) {

                                endedFlow.appProtocol = getNDPIProcol(endedFlow);
                                endedFlow.riskLevel = getNDPIFlowRisk(endedFlow);
                                endedFlow.riskMask = getNDPIFlowRiskMask(endedFlow);
                                endedFlow.riskLabel = getNDPIFlowRiskLabel(endedFlow);
                                endedFlow.riskSeverity = getNDPIFlowRiskSeverity(endedFlow);
                                publishFlow(endedFlow, endFlag);
                                flushedEarlyCount++;
                                logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Flow flushed early due to FIN/RST: {} firstSeen={} lastSeen={} bytes={} packets={}",
                                                endedFlow.key, endedFlow.firstSeen, endedFlow.lastSeen, endedFlow.bytes,
                                                endedFlow.packetCount);
                        }
                }

        }

        private void handleArp(Packet packet, JsonObject json) {
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
                        String key = buildBilateralFlowKey(srcIp, srcPort, dstIp, dstPort, protocol);

                        // Create flow and publish immediately
                        Flow arpFlow = new Flow(key, srcIp, dstIp, srcPort, dstPort, protocol,
                                        json.getLong("timestamp", System.currentTimeMillis()));

                        String arpPackId = setPacketId(srcIp, dstIp, protocol,
                                        json.getLong("timestamp", System.currentTimeMillis()), eth);
                        arpFlow.addPacket(packet, json.getLong("timestamp", System.currentTimeMillis()), arpPackId);
                        arpFlow.packetCount = 1;
                        arpFlow.bytes = (long) eth.length();
                        arpFlow.lastSeen = json.getLong("timestamp", System.currentTimeMillis());
                        arpFlow.firstSeen = arpFlow.lastSeen;
                        arpFlow.appProtocol = "ARP";
                        arpFlow.ndpiFlowPtr = 0;
                        arpFlow.riskLevel = 0;
                        arpFlow.riskMask = 0;
                        arpFlow.riskLabel = "Unknown (ARP)";
                        arpFlow.riskSeverity = "Unknown (ARP)";
                        arpFlow.treatmentDelay.add(System.currentTimeMillis()
                                        - json.getLong("ingestedAt", 0L));
                        publishFlow(arpFlow, "ARP");
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
                                        handleArp(innerEth, json);
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
                return srcIp + "_" + dstIp + "_" + (srcPort != null ? srcPort : "null") + "_"
                                + (dstPort != null ? dstPort : "null") + "_"
                                + (protocol != null ? protocol : "UNKNOWN");
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
                String flowKeyA = buildFlowKey(srcIp, dstIp, srcPort, dstPort, protocol);
                String flowKeyB = buildFlowKey(dstIp, srcIp, dstPort, srcPort, protocol);
                return flowKeyA.compareTo(flowKeyB) <= 0 ? flowKeyA : flowKeyB;
        }

        /**
         * Publish flow to Kafka topic as JSON
         * 
         * @param f               Flow to publish
         * @param reasonOfFlowEnd Reason for flow termination
         */
        private void publishFlow(Flow f, String reasonOfFlowEnd) {
                f.reasonOfFlowEnd = reasonOfFlowEnd;
                f.calculateStats();
                // Enrichissement avant publication
                f.enrich(geoIPService, dnsService, whoisService, vertx)
                                .onSuccess(enrichedFlow -> {
                                        JsonObject jo = enrichedFlow.getJsonObject();
                                        String value = jo.encode();

                                        logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Published flow: key={} protocol={} appProtocol={} riskLevel={} riskLabel={} riskSeverity={} bytes={} packets={} durationMs={} srcCountry={} dstCountry={} srcDomain={} dstDomain={} srcOrg={} dstOrg={}",
                                                        enrichedFlow.key, enrichedFlow.protocol,
                                                        enrichedFlow.appProtocol,
                                                        enrichedFlow.riskLevel, enrichedFlow.riskLabel,
                                                        enrichedFlow.riskSeverity,
                                                        enrichedFlow.bytes, enrichedFlow.packetCount,
                                                        (enrichedFlow.lastSeen - enrichedFlow.firstSeen),
                                                        enrichedFlow.srcCountry, enrichedFlow.dstCountry,
                                                        enrichedFlow.srcDomain, enrichedFlow.dstDomain,
                                                        enrichedFlow.srcOrg, enrichedFlow.dstOrg);
                                        // log treatment delay stats
                                        logger.debug("[ FLOWAGGREGATOR VERTICLE ]       Flow {} treatment delay stats (ns): {}",
                                                        enrichedFlow.key,
                                                        enrichedFlow.treatmentDelay.toString());

                                        KafkaProducerRecord<String, String> record = KafkaProducerRecord
                                                        .create(OUT_TOPIC, enrichedFlow.key, value);

                                        producer.write(record, ar -> {
                                                if (ar.failed()) {
                                                        logger.error("[ FLOWAGGREGATOR VERTICLE ]       Failed to publish flow {}: {}",
                                                                        enrichedFlow.key,
                                                                        ar.cause().getMessage());
                                                }
                                        });
                                })
                                .onFailure(err -> {
                                        logger.warn("[ FLOWAGGREGATOR VERTICLE ]       Enrichment failed for flow {}: {}",
                                                        f.key, err.getMessage());
                                        // Publier quand même le flow non enrichi
                                        JsonObject jo = f.getJsonObject();
                                        producer.write(KafkaProducerRecord.create(OUT_TOPIC, f.key, jo.encode()));
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

}
