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
import java.util.concurrent.CopyOnWriteArrayList;

import com.aut25.vertx.services.DnsService;
import com.aut25.vertx.services.GeoIPService;
import com.aut25.vertx.services.WhoisService;
import com.aut25.vertx.utils.Colors;
import com.aut25.vertx.utils.Flow;
import com.aut25.vertx.utils.NDPIWrapper;
import com.aut25.vertx.utils.NdpiFlowWrapper;

import static java.lang.Thread.sleep;

import io.vertx.core.Future;
import io.vertx.core.Promise;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.vertx.core.shareddata.Lock;

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

        private long total_published_flows_ = 0;

        private KafkaConsumer<String, String> consumer;
        private KafkaProducer<String, String> producer;

        private final Map<String, Flow> flows = new ConcurrentHashMap<>();
        private List<Flow> toFlush = new ArrayList<>();
        private String endFlag = "";
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

        AtomicInteger inFlight = new AtomicInteger(0);
        int MAX_IN_FLIGHT = 1000;

        AtomicLong processed = new AtomicLong(0);
        AtomicLong processed_rate_second = new AtomicLong(0);

        private final ConcurrentHashMap<Integer, Long> lastTsPerPartition = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Integer, AtomicInteger> countPerPartition = new ConcurrentHashMap<>();
        private int partition;
        private final Map<Integer, Long> lastOffsetPerPartition = new HashMap<>();
        private final Map<Integer, Map<String, Long>> lastTsPerPartitionFlow = new HashMap<>();

        private long start;

        /**
         * Start the verticle, initialize Kafka consumer and producer, and set up
         * processing logic
         * 
         * @throws Exception if an error occurs during startup
         */
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
                        logger.debug(Colors.GREEN + "[ FLOWAGGREGATOR VERTICLE ]       Loaded settings from shared map."
                                        + Colors.RESET);
                }
                logger.debug(Colors.GREEN
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

                vertx.sharedData().getLock("ndpi_init_lock", ar -> {
                        if (ar.succeeded()) {
                                Lock lock = ar.result();
                                try {
                                        // LocalMap<String, Object> map = vertx.sharedData().getLocalMap("config");

                                        if (!Boolean.TRUE.equals(map.get("ndpi_initialized"))) {
                                                ndpi.init();
                                                map.put("ndpi_initialized", true);
                                                logger.info("[ FLOWAGGREGATOR VERTICLE ]       nDPI initialized successfully.");
                                        }
                                } catch (Exception e) {
                                        logger.error("[ FLOWAGGREGATOR VERTICLE ]       Error initializing nDPI: {}",
                                                        e.getMessage());
                                } finally {
                                        lock.release();
                                }
                        }
                });

                Map<String, String> consumerConfig = new HashMap<>();
                consumerConfig.put("bootstrap.servers", BOOTSTRAP_SERVERS);
                consumerConfig.put("group.id", GROUP_ID);
                consumerConfig.put("group.instance.id",
                                "flow-aggregator-" + UUID.randomUUID());

                // Désérialisation
                consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

                // Gestion des offsets
                consumerConfig.put("enable.auto.commit", "false"); // Commit manuel recommandé
                consumerConfig.put("auto.offset.reset", "earliest"); // Depuis le début si pas d'offset

                // Lecture efficace
                consumerConfig.put("max.poll.records", "5000"); // Traite 500 messages par poll (au lieu de 1)
                consumerConfig.put("fetch.min.bytes", "65536"); // Attend au moins 32 KB de
                // données
                consumerConfig.put("fetch.max.wait.ms", "20"); // Max 50ms d'attente pour atteindre fetch.min.bytes
                consumerConfig.put("max.partition.fetch.bytes", "2097152"); // 2 MB max par partition

                // Timeouts
                consumerConfig.put("session.timeout.ms", "30000"); // 10s
                consumerConfig.put("heartbeat.interval.ms", "10000"); // 3s
                consumerConfig.put("request.timeout.ms", "1200000"); // 30s, plus large pour gros fetch

                // Buffers réseau
                consumerConfig.put("receive.buffer.bytes", "262144"); // 256 KB
                consumerConfig.put("send.buffer.bytes", "262144"); // 256 KB
                consumerConfig.put("max.poll.interval.ms", "600000");

                // Identification client
                String clientId = "flow-aggregator-" + vertx.getOrCreateContext().deploymentID();
                consumerConfig.put("client.id", clientId);

                consumer = KafkaConsumer.create(vertx, consumerConfig);
                // logger.debug("[ FLOWAGGREGATOR VERTICLE ] Kafka consumer created : " +
                // consumerConfig.toString());

                // Kafka producer config
                Map<String, String> producerConfig = new HashMap<>();
                producerConfig.put("bootstrap.servers", BOOTSTRAP_SERVERS);
                producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                producerConfig.put("acks", "1");
                producerConfig.put("max.request.size", "5242880"); // 5 MB
                producerConfig.put("compression.type", "lz4");

                producer = KafkaProducer.create(vertx, producerConfig);

                // logger.debug("[ FLOWAGGREGATOR VERTICLE ] Kafka producer created : " +
                // producerConfig.toString());

                // Subscribe to input topic
                String verticleId = this.deploymentID();
                consumer
                                .partitionsRevokedHandler(partitions -> {
                                        logger.debug(
                                                        "[FLOWAGGREGATOR VERTICLE {}] Kafka rebalance START - partitions revoked: {}",
                                                        verticleId,
                                                        partitions);
                                })
                                .partitionsAssignedHandler(partitions -> {
                                        logger.debug(
                                                        "[FLOWAGGREGATOR VERTICLE {}] Kafka rebalance END - partitions assigned: {}",
                                                        verticleId,
                                                        partitions);
                                });
                // Subscribe to input topic
                consumer.partitionsAssignedHandler(partitions -> {
                        logger.debug("[FLOW {}] Partitions assigned: {}", deploymentID(), partitions);
                        vertx.eventBus().publish("flow.aggregator.ready", deploymentID());
                })
                                .subscribe(IN_TOPIC, ar -> {
                                        if (ar.succeeded()) {
                                                logger.debug(Colors.CYAN
                                                                + "[ FLOWAGGREGATOR VERTICLE {} ]       Subscribed to topic {}",
                                                                deploymentID(), IN_TOPIC + Colors.RESET);
                                                start = System.nanoTime();

                                        } else {
                                                logger.error("[ FLOWAGGREGATOR VERTICLE ]       Failed to subscribe: {}",
                                                                ar.cause().getMessage());
                                        }
                                });

                // Handler for incoming Kafka messages
                // consumer.pause();
                consumer.handler(record -> {
                        if (!running.get())
                                return;

                        // Debug partition stats
                        partition = record.partition();
                        countPerPartition.computeIfAbsent(partition, k -> new AtomicInteger()).incrementAndGet();

                        if (inFlight.incrementAndGet() > MAX_IN_FLIGHT) {
                                consumer.pause();
                        }
                        String value = record.value();
                        if (value == null || value.isEmpty() || value.equals("reset")) {
                                // Display packet info
                                // logger.debug("[ FLOWAGGREGATOR VERTICLE ] Empty or Reset Packet details: {}",
                                // record);
                                inFlight.decrementAndGet();
                                return;
                        }

                        // vertx.executeBlocking(promise -> {
                        try {
                                // Check for Kafka order violation per partition
                                long offset = record.offset();
                                long lastOffset = lastOffsetPerPartition.getOrDefault(partition, -1L);
                                if (offset <= lastOffset) {
                                        logger.error(
                                                        "[ FLOWAGGREGATOR VERTICLE ] Kafka order violation! partition={} lastOffset={} currentOffset={}",
                                                        partition, lastOffset, offset);
                                }
                                lastOffsetPerPartition.put(partition, offset);

                                JsonObject json = new JsonObject(record.value());
                                long packetTs = json.getLong(
                                                "timestamp",
                                                System.currentTimeMillis());

                                String flowKey = json.getString("flow_id") != null ? json.getString("flow_id")
                                                : "unknown_flow";

                                // Check for timestamp regression per flow
                                Map<String, Long> lastTsPerFlow = lastTsPerPartitionFlow.computeIfAbsent(
                                                partition,
                                                p -> new HashMap<>());

                                long lastTs = lastTsPerFlow.getOrDefault(flowKey, 0L);
                                if (packetTs < lastTs) {
                                        logger.debug(
                                                        "[ FLOWAGGREGATOR VERTICLE ] Flow {} timestamp backward (partition={}): {} -> {}",
                                                        flowKey, partition, lastTs, packetTs);
                                }

                                lastTsPerFlow.put(flowKey, packetTs);

                                try {
                                        processRecord(json);
                                        processed.incrementAndGet();
                                } finally {
                                        int current = inFlight.decrementAndGet();

                                        if (current < MAX_IN_FLIGHT / 2) {
                                                consumer.resume();
                                        }
                                        processed_rate_second.incrementAndGet();
                                }

                                processed.incrementAndGet();
                                // promise.complete();
                        } catch (Exception e) {
                                // promise.fail(e);
                        }
                        // }, false, ar -> {
                        // inFlight.decrementAndGet();
                        // if (inFlight.get() < MAX_IN_FLIGHT / 2) {
                        // consumer.resume();
                        // }

                        // if (ar.failed()) {
                        // logger.error("[ FLOWAGGREGATOR VERTICLE ] Error processing record: {}",
                        // ar.cause());
                        // logger.error("[ FLOWAGGREGATOR VERTICLE ] Offending record: {}",
                        // record.value());
                        // }
                        // });

                });
                // consumer.resume();
                vertx.setPeriodic(100, id -> {
                        if (processed.get() > 0) {
                                consumer.commit(ar -> {
                                        if (ar.failed()) {
                                                logger.error("[ FLOWAGGREGATOR VERTICLE ]       Commit failed: {}",
                                                                ar.cause().getMessage());
                                        }
                                });
                                processed.set(0);
                        }
                });

                // Periodic cleanup task to flush expired flows
                vertx.setPeriodic(FLOW_CLEAN_PERIOD_MS, id -> {
                        if (!running.get())
                                return;
                        loopFlushEveryDelay();
                });

                vertx.setPeriodic(50, id -> {
                        // Log treated packets rate per second
                        long rate = processed_rate_second.getAndSet(0);
                        vertx.eventBus().publish("metrics.rates",
                                        new JsonObject()
                                                        .put("type", "FLOW_AGGREGATION_RATE")
                                                        .put("rate_per_second", rate)
                                                        .put("unit", "packets/s")
                                                        .put("verticle_id", deploymentID()));
                });

                vertx.eventBus().consumer("pcap.global.done", msg -> {
                        logger.warn("[FLOW {}] GLOBAL_PCAP_DONE received", deploymentID());

                        flushRemainingFlowsSafely()
                                        .onSuccess(v -> commitAndShutdown())
                                        .onFailure(err -> logger.error("[FLOW {}] Flush failed", deploymentID(), err));
                });

        }

        private Future<Void> flushRemainingFlowsSafely() {

                Promise<Void> promise = Promise.promise();

                vertx.runOnContext(v -> {
                        try {
                                for (Flow f : new ArrayList<>(flows.values())) {
                                        // nDPI analysis on flow packets if not already detected
                                        f.appProtocol = getNDPIProcol(f);
                                        f.riskLevel = getNDPIFlowRisk(f);
                                        f.riskMask = getNDPIFlowRiskMask(f);
                                        f.riskLabel = getNDPIFlowRiskLabel(f);
                                        f.riskSeverity = getNDPIFlowRiskSeverity(f);

                                        publishFlow(f, "PCAP_DONE");
                                        flows.remove(f.key);
                                        ndpiFlows.remove(f.key);
                                }

                                promise.complete();

                        } catch (Exception e) {
                                promise.fail(e);
                        }
                });

                return promise.future();
        }

        private void commitAndShutdown() {
                consumer.commit(ar -> {
                        if (ar.succeeded()) {
                                logger.info("[FLOW {}] Final commit OK", deploymentID());
                        } else {
                                logger.error("[FLOW {}] Final commit FAILED", deploymentID(), ar.cause());
                        }
                        consumer.close();
                });
        }

        private void loopFlushEveryDelay() {
                // vertx.executeBlocking(promise -> {
                if (!running.get())
                        return;

                Iterator<Flow> iterator = toFlush.iterator();
                while (iterator.hasNext()) {
                        Flow f = iterator.next();
                        // // logger.debug("[ FLOWAGGREGATOR VERTICLE ] Flushing flow: key={} bytes={}
                        // packets={} durationMs={}",
                        // f.key, f.bytes, f.packetCount, (f.lastSeen - f.firstSeen));

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
                // promise.complete();
                // }, false, res -> {
                // if (res.failed()) {
                // logger.error("[ FLOWAGGREGATOR VERTICLE ] Error in flush loop: {}",
                // res.cause());
                // }
                // });
        }

        /**
         * Publish a flow to the output Kafka topic
         * 
         * @param f Flow to publish
         */
        private void flushExpiredFlows(long referenceTimeMs) {
                if (!running.get())
                        return;

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
                                + "[ FLOWAGGREGATOR VERTICLE {} ]       Flushing remaining flows before shutdown...",
                                deploymentID() + Colors.RESET);
                toFlush = new ArrayList<>(flows.values());
                // Display how many packet still in queue

                Iterator<Flow> iterator = toFlush.iterator();
                while (iterator.hasNext()) {
                        Flow f = iterator.next();
                        // // logger.debug("[ FLOWAGGREGATOR VERTICLE ] Flushing remaining flow: key={}
                        // bytes={} packets={} durationMs={}",
                        // f.key, f.bytes, f.packetCount, (f.lastSeen - f.firstSeen));

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
                // Remaining
                logger.info(Colors.CYAN
                                + "[ FLOWAGGREGATOR VERTICLE {} ]       Flush complete. Total published flows before shutdown: {}",
                                deploymentID(), total_published_flows_ + Colors.RESET);
                // print toflush

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
                // if ("PCAP_DONE".equals(json.getString("status"))) {

                // vertx.executeBlocking(promise -> {
                // while (inFlight.get() > 0) {
                // try {
                // Thread.sleep(100);
                // } catch (InterruptedException e) {
                // // Ignore
                // }
                // }
                // consumer.pause();
                // flushRemainingFlows();
                // }, res -> {
                // consumer.commit(ar -> {
                // logger.info("[FLOWAGGREGATOR {}] Shutdown complete", deploymentID());
                // });
                // if (res.failed()) {
                // logger.error("[ FLOWAGGREGATOR VERTICLE ] Error during PCAP_DONE flush: {}",
                // res.cause().getMessage());
                // } else {
                // logger.info(Colors.CYAN + "Remaining flows to flush: {}", toFlush.size());
                // logger.info(Colors.CYAN + "Remaining flows in map: {}", flows.size());
                // }
                // });

                // return;

                // }
                if ("PCAP_DONE".equals(json.getString("status"))) {

                        logger.debug("[FLOW {}] PCAP_DONE received for partition {}",
                                        deploymentID(), partition);

                        // Stop consuming further records for this partition
                        consumer.pause();

                        // Signale au coordinateur que CETTE partition est terminée
                        vertx.eventBus().send("pcap.partition.done",
                                        new JsonObject()
                                                        .put("verticleId", deploymentID())
                                                        .put("partition", partition)
                                                        .put("startTime", start));

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
                                handleIPv4(packet, json, rawData);
                                break;

                        case 0x86DD: // IPv6
                                handleIPv6(packet, json, rawData);
                                break;

                        case 0x0806: // ARP
                                handleArp(packet, json);
                                break;

                        case 0x8100: // VLAN
                                handleVlanEncapsulated(packet, json, rawData);
                                break;

                        default:
                                // logger.debug("[FLOWAGGREGATOR] Unsupported EtherType: 0x{}. Raw data: {}",
                                // Integer.toHexString(etherType),
                                // Base64.getEncoder().encodeToString(rawData));
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

                        ndpiFlow.detectedProtocol = ndpiProtocol;

                } catch (Exception e) {
                        logger.warn("[ FLOWAGGREGATOR VERTICLE ] Could not analyze flow with nDPI: {}",
                                        e.getMessage());
                }
                // Update or create flow
                flows.compute(key, (k, f) -> {
                        // Create treatmentDelay JsonObject if null

                        if (f == null) {
                                f = new Flow(k, srcIp, dstIp, srcPort, dstPort, protocol, ts);
                                f.ndpiFlowPtr = ndpiFlow.ndpiFlowPtr;
                                f.treatmentDelay = new CopyOnWriteArrayList<>();
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
                        if (ipPacket.contains(TcpPacket.class)) {
                                TcpPacket tcp = ipPacket.get(TcpPacket.class);
                                TcpPacket.TcpHeader tcpHeader = tcp.getHeader();

                                if (tcpHeader.getFin()) {
                                        if (srcIp.equals(f.srcIp) && srcPort.equals(f.srcPort)) {
                                                f.finFromSrc = true;
                                                f.finSrcSeq = tcpHeader.getSequenceNumber();
                                        } else {
                                                f.finFromDst = true;
                                                f.finDstSeq = tcpHeader.getSequenceNumber();
                                        }
                                } else if (tcpHeader.getAck()) {

                                        long ack = tcpHeader.getAcknowledgmentNumber();

                                        // ACK du FIN SRC
                                        if (f.finFromSrc && !f.finAckedByDst &&
                                                        ack == f.finSrcSeq + 1 &&
                                                        srcIp.equals(f.dstIp)) {
                                                f.finAckedByDst = true;
                                        }

                                        // ACK du FIN DST
                                        if (f.finFromDst && !f.finAckedBySrc &&
                                                        ack == f.finDstSeq + 1 &&
                                                        srcIp.equals(f.srcIp)) {
                                                f.finAckedBySrc = true;
                                        }
                                }

                                // if TCP RST flag is set, end flow immediately
                                if (tcpHeader.getRst()) {
                                        flowEnded.set(true);
                                        endFlag = "TCP RST";
                                }

                                // Check if flow is properly closed
                                if (f.isProperlyClosed()) {
                                        flowEnded.set(true);
                                        endFlag = "TCP FIN";
                                }

                        }

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
         * Get the outer source port from an IP packet.
         * 
         * @param ipPacket
         * @return The outer source port as an integer.
         */
        private int getOuterSrcPort(IpPacket ipPacket) {
                Packet payload = ipPacket.getPayload();
                if (payload instanceof TcpPacket) {
                        return ((TcpPacket) payload).getHeader().getSrcPort().valueAsInt();
                } else if (payload instanceof UdpPacket) {
                        return ((UdpPacket) payload).getHeader().getSrcPort().valueAsInt();
                }
                return 0;
        }

        /**
         * Get the outer destination port from an IP packet.
         * 
         * @param ipPacket
         * @return The outer destination port as an integer.
         */
        private int getOuterDstPort(IpPacket ipPacket) {
                Packet payload = ipPacket.getPayload();
                if (payload instanceof TcpPacket) {
                        return ((TcpPacket) payload).getHeader().getDstPort().valueAsInt();
                } else if (payload instanceof UdpPacket) {
                        return ((UdpPacket) payload).getHeader().getDstPort().valueAsInt();
                }
                return 0;
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
                        return new Ports(getOuterSrcPort((IpPacket) ipPacket),
                                        getOuterDstPort((IpPacket) ipPacket));
                } else if (ipPacket.getPayload() instanceof UdpPacket udp) {
                        return new Ports(getOuterSrcPort((IpPacket) ipPacket),
                                        getOuterDstPort((IpPacket) ipPacket));
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
                total_published_flows_++;
                // Enrichissement avant publication

                f.enrich(geoIPService, dnsService, whoisService, vertx)
                                .onSuccess(enrichedFlow -> {
                                        JsonObject jo = enrichedFlow.getJsonObject();
                                        String value = jo.encode();

                                        // logger.debug("[ FLOWAGGREGATOR VERTICLE ] Published flow: key={} protocol={}
                                        // appProtocol={} riskLevel={} riskLabel={} riskSeverity={} bytes={} packets={}
                                        // durationMs={} srcCountry={} dstCountry={} srcDomain={} dstDomain={} srcOrg={}
                                        // dstOrg={}",
                                        // enrichedFlow.key, enrichedFlow.protocol,
                                        // enrichedFlow.appProtocol,
                                        // enrichedFlow.riskLevel, enrichedFlow.riskLabel,
                                        // enrichedFlow.riskSeverity,
                                        // enrichedFlow.bytes, enrichedFlow.packetCount,
                                        // (enrichedFlow.lastSeen - enrichedFlow.firstSeen),
                                        // enrichedFlow.srcCountry, enrichedFlow.dstCountry,
                                        // enrichedFlow.srcDomain, enrichedFlow.dstDomain,
                                        // enrichedFlow.srcOrg, enrichedFlow.dstOrg);

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
                                        producer.write(KafkaProducerRecord.create(OUT_TOPIC, f.key,
                                                        jo.encode()));
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
