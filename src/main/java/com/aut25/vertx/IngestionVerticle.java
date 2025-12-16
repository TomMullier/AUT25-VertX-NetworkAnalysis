package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Iterator;
import java.io.EOFException;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.util.PriorityQueue;
import java.util.Comparator;

import com.aut25.vertx.FlowAggregatorVerticle.Ports;
import com.aut25.vertx.utils.Colors;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.pcap4j.core.PacketListener;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.PcapNetworkInterface;
import org.pcap4j.core.Pcaps;
import org.pcap4j.packet.IpPacket;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.TcpPacket;
import org.pcap4j.packet.UdpPacket;
import org.pcap4j.packet.namednumber.DataLinkType;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Enumeration;
import java.util.concurrent.Executors;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.PriorityQueue;
import java.util.Comparator;

public class IngestionVerticle extends AbstractVerticle {

        private static final Logger logger = LoggerFactory.getLogger(IngestionVerticle.class);
        private KafkaProducer<String, String> producer;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private JsonObject config;

        private class PacketWrapper {
                Packet packet;
                long timestamp;
                long delta;
                long index;

                public PacketWrapper(Packet packet, long timestamp, long delta, long index) {
                        this.packet = packet;
                        this.timestamp = timestamp;
                        this.delta = delta;
                        this.index = index;

                }
        }

        private PriorityQueue<PacketWrapper> packetQueue = new PriorityQueue<>();

        @Override
        public void start() throws Exception {
                logger.info(Colors.GREEN + "[ INGESTION VERTICLE ]            Starting IngestionVerticle..."
                                + Colors.RESET);

                // Config file : get debug mode
                try {
                        LocalMap<String, Object> map = vertx.sharedData().getLocalMap("config");
                        config = new JsonObject(map);
                        logger.debug("[ INGESTION VERTICLE ] Loaded configuration: " + config.encodePrettily());

                } catch (Exception e) {
                        logger.error("[ INGESTION VERTICLE ]            Failed to read config file: "
                                        + e.getMessage());
                        return;
                }

                /*
                 * Get mode from config file.
                 * Can be :
                 * - json (default for debug)
                 * - pcap
                 * - realtime
                 */
                String mode;

                if (config != null && config.containsKey("ingestionMethod")) {
                        mode = config.getString("ingestionMethod", "json").toLowerCase();
                } else {
                        mode = config.getString("mode", "json").toLowerCase();
                }
                if (!List.of("json", "pcap", "realtime").contains(mode)) {
                        logger.error("[ INGESTION VERTICLE ]            Invalid mode specified: " + mode);
                        return;
                }
                logger.info(Colors.GREEN + "[ INGESTION VERTICLE ]            Ingestion mode: " + mode.toUpperCase()
                                + Colors.RESET);

                /* ----------------------- Creation of Kafka producer ----------------------- */
                configureKafkaProducer();
                KafkaProducerRecord<String, String> resetRecord = KafkaProducerRecord.create("network-data", "reset");
                producer.send(resetRecord, ar -> {
                        if (ar.succeeded()) {
                                logger.debug(Colors.CYAN
                                                + "[ INGESTION VERTICLE ]            Kafka topic 'network-data' reset successfully."
                                                + Colors.RESET);
                        } else {
                                logger.error("[ INGESTION VERTICLE ]            Failed to reset Kafka topic: "
                                                + ar.cause().getMessage());
                        }
                });

                logger.debug(Colors.YELLOW + "[ INGESTION VERTICLE ][ CONFIG ]  Mode: " + mode.toUpperCase()
                                + Colors.RESET);
                switch (mode) {
                        case "json":
                                JsonObject fileConfig = config.getJsonObject("json", new JsonObject());
                                String filePath = fileConfig.getString("file-path", "data/sample_data.json");
                                int interval = fileConfig.getInteger("ingestion-interval-ms", 1000);

                                logger.debug("[ INGESTION VERTICLE ][ CONFIG ] File path: " + filePath);
                                logger.debug("[ INGESTION VERTICLE ][ CONFIG ] Ingestion interval (ms): " + interval);

                                List<JsonObject> records;
                                try {
                                        String content = Files.readString(Paths.get(filePath), StandardCharsets.UTF_8);
                                        ObjectMapper mapper = new ObjectMapper();
                                        List<Map<String, Object>> listOfMaps = mapper.readValue(
                                                        content, new TypeReference<List<Map<String, Object>>>() {
                                                        });
                                        records = listOfMaps.stream()
                                                        .map(JsonObject::mapFrom)
                                                        .toList();
                                } catch (Exception e) {
                                        logger.error("[ INGESTION VERTICLE ]            Failed to read or parse file: "
                                                        + e.getMessage());
                                        return;
                                }

                                AtomicReference<Iterator<JsonObject>> iteratorRef = new AtomicReference<>(
                                                records.iterator());
                                vertx.setPeriodic(interval, id -> {
                                        if (!running.get())
                                                return;

                                        Iterator<JsonObject> it = iteratorRef.get();
                                        if (!it.hasNext()) {
                                                it = records.iterator();
                                                iteratorRef.set(it);
                                                logger.debug("[ INGESTION VERTICLE ]            End of file reached. Looping again...");
                                        }

                                        JsonObject record = it.next();
                                        KafkaProducerRecord<String, String> kafkaRecord = KafkaProducerRecord
                                                        .create("network-data", record.encode());
                                        producer.send(kafkaRecord, ar -> {
                                                if (ar.succeeded()) {
                                                } else {
                                                        logger.error("[ INGESTION VERTICLE ]            Failed to send record to Kafka: "
                                                                        + ar.cause().getMessage());
                                                }
                                        });
                                });
                                break;

                        case "pcap":
                                // TODO : implement pcap replay for ingestion
                                JsonObject pcapConfig = config.getJsonObject("pcap", new JsonObject());
                                String pcapFilePath = pcapConfig.getString("file-path",
                                                "src/main/resources/datapcap-sample.pcap");
                                logger.debug("[ INGESTION VERTICLE ][ CONFIG ] PCAP File path: " + pcapFilePath);

                                ingestFromPcap(pcapFilePath);

                                break;

                        case "realtime":
                                JsonObject rtConfig = config.getJsonObject("realtime", new JsonObject());
                                String networkInterface = rtConfig.getString("interface", null);
                                if (networkInterface == null) {
                                        networkInterface = chooseNetworkinterface();
                                }
                                logger.debug("[ INGESTION VERTICLE ][ CONFIG ] Network Interface: "
                                                + networkInterface);
                                // Put in shared data
                                vertx.sharedData().getLocalMap("config").put("realtime", new JsonObject()
                                                .put("interface", networkInterface));
                                ingestInRealTime(networkInterface);
                                break;
                        case "none":
                                logger.warn(Colors.YELLOW
                                                + "[ INGESTION VERTICLE ]            Mode is set to 'none', no ingestion will be performed."
                                                + Colors.RESET);
                                return;
                        default:
                                logger.error(
                                                "[ INGESTION VERTICLE ]            Unknown mode. No ingestion will be performed.");
                                break;
                }

                logger.debug("[ INGESTION VERTICLE ]            IngestionVerticle started!");

        }

        private String chooseNetworkinterface() {
                try {

                        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                        List<NetworkInterface> interfaceList = new ArrayList<>();

                        logger.info(Colors.MAGENTA
                                        + "[ INGESTION VERTICLE ]            Available network interfaces:"
                                        + Colors.RESET);
                        int index = 1;
                        while (interfaces.hasMoreElements()) {
                                NetworkInterface ni = interfaces.nextElement();
                                // Ignore loopback and down interfaces
                                if (ni.isUp() && !ni.isLoopback()) {
                                        interfaceList.add(ni);
                                        logger.info(Colors.MAGENTA + "                                  [" + index
                                                        + "] " + ni.getName() + " ("
                                                        + ni.getDisplayName() + ")" + Colors.RESET);
                                        index++;
                                }
                        }

                        if (interfaceList.isEmpty()) {
                                throw new RuntimeException("No active network interfaces found.");
                        }

                        Scanner scanner = new Scanner(System.in);
                        int choice = -1;
                        try {
                                while (choice < 1 || choice > interfaceList.size()) {
                                        logger.info(Colors.MAGENTA
                                                        + "[ INGESTION VERTICLE ]            Select a network interface by number (1-"
                                                        + interfaceList.size() + "): "
                                                        + Colors.RESET);
                                        if (scanner.hasNextInt()) {
                                                choice = scanner.nextInt();
                                        } else {
                                                scanner.next(); // ignore non-numeric input
                                        }
                                }
                        } finally {
                                // Do not close the scanner to avoid closing System.in
                        }

                        logger.info(Colors.MAGENTA + "[ INGESTION VERTICLE ]            Selected interface: "
                                        + interfaceList.get(choice - 1).getName() + Colors.RESET);

                        return interfaceList.get(choice - 1).getName();
                } catch (Exception e) {
                        logger.error("[ INGESTION VERTICLE ]            Error retrieving network interfaces: "
                                        + e.getMessage());
                        return null;
                }

        }

        /* ------------------- Configuration of the Kafka producer ------------------ */
        /**
         * Configure Kafka producer with necessary properties
         */
        private void configureKafkaProducer() {
                Properties props = new Properties();
                props.put("bootstrap.servers", "localhost:9092");

                // Serializers classiques
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

                // ACK minimal pour aller plus vite
                props.put("acks", "1"); // pas 0 car risque de perte sans gain significatif

                // === LATENCE ULTRA-FAIBLE ===

                // Désactiver totalement le batching côté producer
                props.put("batch.size", "16384"); // 16KB batch size
                props.put("linger.ms", "1"); // 1ms linger time
                props.put("buffer.memory", "33554432"); // 32MB pour absorber les bursts

                props.put("compression.type", "lz4"); // rapide et efficace

                // Réutiliser les connexions TCP
                props.put("connections.max.idle.ms", "300000");

                // Activer le transfert direct sans mise en attente
                props.put("max.in.flight.requests.per.connection", "1");
                // → évite reordering, réduit jitter

                // Temps de réponse maximal très bas
                props.put("request.timeout.ms", "15000");
                props.put("delivery.timeout.ms", "20000"); // global timeout

                // TCP optimisé latence
                props.put("socket.send.buffer.bytes", "32768"); // 32 KB
                props.put("socket.receive.buffer.bytes", "32768");

                // Moins de copies mémoire
                props.put("message.max.bytes", "1048576"); // 1 MB par message max

                // Le must pour la latence = désactiver la mise en commun des buffers
                props.put("receive.buffer.bytes", "32768");
                props.put("send.buffer.bytes", "32768");

                // Id client pour debug
                props.put("client.id", "low-latency-producer");

                producer = KafkaProducer.create(vertx, props);
                logger.debug("[ INGESTION VERTICLE ]            Kafka Producer for ingestion configured."
                                + Colors.RESET);
                logger.debug("[ INGESTION VERTICLE ]            Kafka Producer properties: " + props.toString());
        }

        /* ----------------------------- Mode Realtime ------------------------------ */
        static class Task implements Runnable {
                private final PcapHandle handle;
                private final PacketListener listener;

                Task(PcapHandle handle, PacketListener listener) {
                        this.handle = handle;
                        this.listener = listener;
                }

                @Override
                public void run() {
                        try {
                                handle.loop(-1, listener); // -1 = capture infinie
                        } catch (Exception e) {
                                e.printStackTrace();
                        } finally {
                                try {
                                        handle.close();
                                } catch (Exception ignored) {
                                }
                        }
                }
        }

        /**
         * Ingest packets in real-time (e.g., from a network interface) and publish them
         * to Kafka.
         */
        private void ingestInRealTime(String networkInterface) {
                try {
                        if (running.get() == false)
                                return;
                        PcapNetworkInterface nif = Pcaps.getDevByName(networkInterface);
                        if (nif == null) {
                                logger.error("[ INGESTION VERTICLE ]            Network interface not found: "
                                                + networkInterface);
                                return;
                        }

                        final int snapLen = 65536;
                        final int timeout = 10;
                        PcapHandle handle = nif.openLive(snapLen, PcapNetworkInterface.PromiscuousMode.PROMISCUOUS,
                                        timeout);

                        AtomicReference<Long> timestamp = new AtomicReference<>(System.currentTimeMillis());
                        PacketListener listener = packet -> {
                                long packetTimestamp = System.currentTimeMillis();
                                long delay = packetTimestamp - timestamp.get();
                                timestamp.set(packetTimestamp);
                                processPacket(packet, delay, packetTimestamp);
                        };

                        ExecutorService pool = Executors.newSingleThreadExecutor();
                        pool.execute(() -> {
                                try {
                                        handle.loop(-1, listener);
                                } catch (Exception e) {
                                        e.printStackTrace();
                                }
                        });

                } catch (PcapNativeException e) {
                        logger.error("[ INGESTION VERTICLE ]            Error accessing network interface: "
                                        + e.getMessage());
                }
        }

        /* -------------------------------- Mode pcap ------------------------------- */
        /**
         * Ingest packets from a pcap file and publish them to Kafka, maintaining the
         * original
         * timing between packets.
         * 
         * @param pcapFilePath Path to the pcap file
         */
        private long icmpCount = 0;

        private void ingestFromPcap(String pcapFilePath) {
                PcapHandle handle;
                try {
                        handle = Pcaps.openOffline(pcapFilePath, PcapHandle.TimestampPrecision.NANO);
                } catch (PcapNativeException e) {
                        logger.error("[ INGESTION VERTICLE ]            Failed to open pcap file: " + e.getMessage());
                        return;
                }

                logger.info("[ INGESTION VERTICLE ]            PCAP file opened successfully: {}", pcapFilePath);
                DataLinkType dlt = handle.getDlt();
                logger.debug("[ INGESTION VERTICLE ]            Data Link Type: {}", dlt);
                vertx.executeBlocking(promise -> {
                        try {
                                if (running.get() == false) {
                                        handle.close();
                                        promise.complete();
                                        return;
                                }
                                // Lire tous les paquets et calculer les deltas de temps
                                List<Packet> packets = new ArrayList<>();
                                List<Long> deltas = new ArrayList<>();
                                List<Long> timestamps = new ArrayList<>();

                                Packet firstPacket = handle.getNextPacketEx();
                                if (firstPacket == null) {
                                        logger.warn("[ INGESTION VERTICLE ]            No packets found in file {}",
                                                        pcapFilePath);
                                        promise.complete();
                                        return;
                                }

                                Timestamp firstTimestamp = handle.getTimestamp();
                                long previousTime = firstTimestamp.getTime();
                                packets.add(firstPacket);
                                deltas.add(0L);
                                timestamps.add(firstTimestamp.getTime());

                                while (true) {
                                        try {
                                                if (!running.get())
                                                        break;
                                                Packet packet = handle.getNextPacketEx();
                                                if (packet == null)
                                                        break;

                                                Timestamp currentTs = handle.getTimestamp();
                                                long delta = currentTs.getTime() - previousTime;
                                                previousTime = currentTs.getTime();

                                                packets.add(packet);
                                                deltas.add(delta);
                                                timestamps.add(currentTs.getTime());

                                        } catch (EOFException e) {
                                                break;
                                        }
                                }

                                handle.close();
                                logger.info(Colors.MAGENTA + "[ INGESTION VERTICLE ]            Total packets read: {}",
                                                packets.size() +
                                                                Colors.RESET);
                                logger.info(Colors.MAGENTA
                                                + "[ INGESTION VERTICLE ]            End of pcap file reached."
                                                + Colors.RESET);
                                promise.complete(List.of(packets, deltas, timestamps));
                        } catch (Exception e) {
                                logger.error("[ INGESTION VERTICLE ]            Error reading pcap: " + e.getMessage());
                                handle.close();
                                promise.fail(e);
                        }
                }, false, res -> {
                        if (res.succeeded()) {
                                @SuppressWarnings("unchecked")
                                List<Object> result = (List<Object>) res.result();
                                @SuppressWarnings("unchecked")
                                List<Packet> packets = (List<Packet>) result.get(0);
                                @SuppressWarnings("unchecked")
                                List<Long> deltas = (List<Long>) result.get(1);
                                @SuppressWarnings("unchecked")
                                List<Long> timestamps = (List<Long>) result.get(2);
                                // Après avoir rempli packets, deltas, timestamps
                                packetQueue = new PriorityQueue<>(
                                                Comparator.comparingLong((PacketWrapper pw) -> pw.timestamp)
                                                                .thenComparingLong(pw -> pw.index));

                                Iterator<Packet> packetIterator = packets.iterator();
                                Iterator<Long> deltaIterator = deltas.iterator();
                                Iterator<Long> timestampIterator = timestamps.iterator();
                                long index = 0;
                                while (packetIterator.hasNext() && deltaIterator.hasNext()
                                                && timestampIterator.hasNext()) {
                                        packetQueue.add(new PacketWrapper(packetIterator.next(),
                                                        timestampIterator.next(),
                                                        deltaIterator.next(),
                                                        index));
                                        index++;
                                }

                                // Lancer la publication
                                publishNextFromQueue();
                        } else {
                                logger.error("[ INGESTION VERTICLE ]            Failed to process pcap file: "
                                                + res.cause().getMessage());
                        }
                });
        }

        private void publishNextFromQueue() {
                if (!running.get() || packetQueue.isEmpty()) {
                        // Tous les paquets traités, envoyer PCAP_DONE
                        JsonObject doneMessage = new JsonObject().put("status", "PCAP_DONE");
                        KafkaProducerRecord<String, String> kafkaRecord = KafkaProducerRecord.create("network-data",
                                        doneMessage.encode());
                        producer.send(kafkaRecord, ar -> {
                                if (ar.succeeded()) {
                                        logger.info("[INGESTION] PCAP finished message sent to Kafka.");
                                } else {
                                        logger.error("[INGESTION] Failed to send PCAP_DONE message: "
                                                        + ar.cause().getMessage());
                                }
                        });
                        return;
                }

                PacketWrapper pw = packetQueue.poll(); // récupère le paquet avec le plus petit timestamp
                long safeDelay = Math.max(pw.delta, 1);

                vertx.setTimer(safeDelay, id -> {
                        processPacket(pw.packet, safeDelay, pw.timestamp);
                        publishNextFromQueue(); // récursion pour le paquet suivant
                });
        }

        /**
         * Process and send a packet to Kafka
         *
         * @param packet The packet to process
         */
        private void processPacket(Packet packet, long delay, long packetTimestamp) {
                if (packet == null)
                        return;

                String base64Packet = Base64.getEncoder().encodeToString(packet.getRawData());
                String flowKey = buildFlowKey(packet); // build flow key
                JsonObject record = new JsonObject()
                                .put("timestamp", packetTimestamp)
                                .put("ingestedAt", System.currentTimeMillis())
                                .put("delay", delay)
                                .put("rawPacket", base64Packet)
                                .put("flow_id", flowKey);
                // Key to partition by flow

                // Create a KafkaRecord with key (key = flowKey)
                KafkaProducerRecord<String, String> kafkaRecord = KafkaProducerRecord.create("network-data", flowKey,
                                record.encode());
                if (!running.get())
                        return;
                producer.send(kafkaRecord, ar -> {
                        if (ar.failed()) {
                                logger.error("[ INGESTION VERTICLE ]            Failed to send packet record: "
                                                + ar.cause().getMessage());
                        }
                });
        }

        /* -------------------------------------------------------------------------- */
        /**
         * Cleanup resources on verticle stop
         */
        @Override
        public void stop() throws Exception {
                running.set(false);
                if (producer != null) {
                        producer.close();
                }
                logger.info(Colors.RED + "[ INGESTION VERTICLE ]            IngestionVerticle stopped!" + Colors.RESET);
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
         * Build a flow key from a packet
         * 
         * @param packet the packet to build the flow key from
         * @return the constructed flow key
         */
        private String buildFlowKey(Packet packet) {
                if (packet.contains(IpPacket.class)) {
                        IpPacket ipPacket = packet.get(IpPacket.class);
                        String srcIp = ipPacket.getHeader().getSrcAddr().getHostAddress();
                        String dstIp = ipPacket.getHeader().getDstAddr().getHostAddress();
                        Ports ports = getPacketPorts(ipPacket);
                        Integer srcPort = ports.src();
                        Integer dstPort = ports.dst();
                        String protocol = ipPacket.getHeader().getProtocol().name();
                        return buildBilateralFlowKey(srcIp, srcPort, dstIp, dstPort, protocol);

                }
                return null;
        }
}
