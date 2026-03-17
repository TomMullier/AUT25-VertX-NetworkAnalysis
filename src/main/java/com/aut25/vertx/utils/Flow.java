package com.aut25.vertx.utils;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.IpPacket;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.TcpPacket;
import org.pcap4j.packet.UdpPacket;
import org.pcap4j.packet.factory.PacketFactories;
import org.pcap4j.packet.namednumber.DataLinkType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.CompositeFuture;
import com.aut25.vertx.services.GeoIPService;
import com.aut25.vertx.services.DnsService;
import com.aut25.vertx.services.WhoisService;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.exception.AddressNotFoundException;

/**
 * Internal Flow class to hold flow state
 * Represents a network flow with its attributes and timestamps.
 */
public class Flow {

        public boolean finFromSrc = false;
        public boolean finAckedByDst = false;
        public boolean finFromDst = false;
        public boolean finAckedBySrc = false;

        public long finSrcSeq = -1;
        public long finDstSeq = -1;

        // logger

        public String srcIp = "";
        public String dstIp = "";
        public Integer srcPort = -1;
        public Integer dstPort = -1;
        public String protocol = "";
        public String key = "";

        public CopyOnWriteArrayList<Long> treatmentDelay = new CopyOnWriteArrayList<>();

        public long firstSeen = -1;
        public long lastSeen = -1;
        public long bytes = -1;
        public long packetCount = -1;

        // Stats of flow
        public double minPacketLength = -1;
        public double maxPacketLength = -1;
        public double meanPacketLength = -1;
        public double stddevPacketLength = -1;

        public double bytesPerSecond = -1;
        public double packetsPerSecond = -1;

        public double totalBytesUpstream = -1;
        public double totalBytesDownstream = -1;
        public double totalPacketsUpstream = -1;
        public double totalPacketsDownstream = -1;
        public double ratioBytesUpstreamDownstream = -1;
        public double ratioPacketsUpstreamDownstream = -1;

        public double flowDurationMs = -1;

        public double interArrivalTimeMean = 0;
        public double interArrivalTimeStdDev = 0;
        public double interArrivalTimeMin = 0;
        public double interArrivalTimeMax = 0;

        // IDS-friendly features
        public double flowSymmetry = -1;
        public double synRate = -1;
        public double finRate = -1;
        public double rstRate = -1;
        public double ackRate = -1;
        public double pshRate = -1;

        // Flags counters
        private long synCount = 0, finCount = 0, rstCount = 0, ackCount = 0, pshCount = 0;
        private long tcpCount = 0, udpCount = 0, otherCount = 0;

        public double tcpFraction = -1;
        public double udpFraction = -1;
        public double otherFraction = -1;

        // List of packets for this flow
        public CopyOnWriteArrayList<Packet> packets = new CopyOnWriteArrayList<>();
        public CopyOnWriteArrayList<String> packetSummaries = new CopyOnWriteArrayList<>();
        public CopyOnWriteArrayList<Long> packetTimestamps = new CopyOnWriteArrayList<>();

        public long ndpiFlowPtr = 0;
        public final CopyOnWriteArrayList<byte[]> packetList = new CopyOnWriteArrayList<>();
        public String appProtocol;
        public byte[] appProtocolBytes;

        public int riskLevel = -1;
        public int riskMask = -1;
        public String riskLabel = "UNKNOWN";
        public String riskSeverity = "UNKNOWN";

        public String reasonOfFlowEnd = "";

        // Enrich flow
        public String srcDomain;
        public String dstDomain;
        public String srcCountry;
        public String dstCountry;
        public String srcOrg;
        public String dstOrg;

        public Logger logger = LoggerFactory.getLogger(Flow.class);

        /**
         * Constructor for Flow
         * 
         * @param key      Flow key (e.g., "srcIp:srcPort-dstIp:dstPort-protocol")
         * @param srcIp    Source IP address
         * @param dstIp    Destination IP address
         * @param srcPort  Source port number
         * @param dstPort  Destination port number
         * @param protocol Protocol used (e.g., TCP, UDP)
         * @param ts       Timestamp of the flow creation
         */
        public Flow(String key, String srcIp, String dstIp, Integer srcPort, Integer dstPort, String protocol,
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
                this.appProtocol = "UNKNOWN";
                this.appProtocolBytes = new byte[0];
                this.riskLevel = -1;
                this.riskMask = -1;
                this.riskLabel = "UNKNOWN";
                this.riskSeverity = "UNKNOWN";

        }

        /**
         * Convert flow attributes to JsonObject
         * 
         * @return JsonObject representation of the flow
         */
        public JsonObject getJsonObject() {
                JsonObject jo = new JsonObject();
                jo.put("firstSeen", this.firstSeen);
                jo.put("lastSeen", this.lastSeen);
                jo.put("srcIp", this.srcIp);
                jo.put("dstIp", this.dstIp);
                jo.put("srcPort", this.srcPort);
                jo.put("dstPort", this.dstPort);
                jo.put("protocol", this.protocol);
                jo.put("bytes", this.bytes);
                jo.put("packetCount", this.packetCount);
                jo.put("flowKey", this.key);

                // Stats (toujours en double)
                jo.put("minPacketLength", (double) this.minPacketLength);
                jo.put("maxPacketLength", (double) this.maxPacketLength);
                jo.put("meanPacketLength", (double) this.meanPacketLength);
                jo.put("stddevPacketLength", (double) this.stddevPacketLength);

                jo.put("bytesPerSecond", (double) this.bytesPerSecond);
                jo.put("packetsPerSecond", (double) this.packetsPerSecond);

                jo.put("totalBytesUpstream", (double) this.totalBytesUpstream);
                jo.put("totalBytesDownstream", (double) this.totalBytesDownstream);
                jo.put("totalPacketsUpstream", (double) this.totalPacketsUpstream);
                jo.put("totalPacketsDownstream", (double) this.totalPacketsDownstream);
                jo.put("ratioBytesUpDown", (double) this.ratioBytesUpstreamDownstream);
                jo.put("ratioPacketsUpDown", (double) this.ratioPacketsUpstreamDownstream);

                jo.put("flowDurationMs", (double) (this.lastSeen - this.firstSeen));

                jo.put("interArrivalTimeMean", (double) this.interArrivalTimeMean);
                jo.put("interArrivalTimeStdDev", (double) this.interArrivalTimeStdDev);
                jo.put("interArrivalTimeMin", (double) this.interArrivalTimeMin);
                jo.put("interArrivalTimeMax", (double) this.interArrivalTimeMax);

                // IDS extra
                jo.put("flowSymmetry", (double) this.flowSymmetry);
                jo.put("synRate", (double) this.synRate);
                jo.put("finRate", (double) this.finRate);
                jo.put("rstRate", (double) this.rstRate);
                jo.put("ackRate", (double) this.ackRate);
                jo.put("pshRate", (double) this.pshRate);

                jo.put("synCount", this.synCount);
                jo.put("finCount", this.finCount);
                jo.put("rstCount", this.rstCount);
                jo.put("ackCount", this.ackCount);
                jo.put("pshCount", this.pshCount);

                jo.put("tcpFraction", (double) this.tcpFraction);
                jo.put("udpFraction", (double) this.udpFraction);
                jo.put("otherFraction", (double) this.otherFraction);

                jo.put("appProtocol", this.appProtocol);
                jo.put("appProtocolBytes", this.appProtocolBytes != null ? this.appProtocolBytes.length : 0);
                jo.put("ndpiFlowPtr", this.ndpiFlowPtr);
                jo.put("riskLevel", this.riskLevel);
                jo.put("riskMask", this.riskMask);
                jo.put("riskLabel", this.riskLabel);
                jo.put("riskSeverity", this.riskSeverity);

                jo.put("packetSummariesString", this.getPacketSummariesString());
                jo.put("packetSummaries", this.getPacketSummaries());
                jo.put("reasonOfFlowEnd", this.reasonOfFlowEnd);

                jo.put("srcCountry", this.srcCountry);
                jo.put("dstCountry", this.dstCountry);
                jo.put("srcOrg", this.srcOrg);
                jo.put("dstOrg", this.dstOrg);
                jo.put("srcDomain", this.srcDomain);
                jo.put("dstDomain", this.dstDomain);
                jo.put("treatmentDelay", new JsonArray(this.treatmentDelay));

                return jo;
        }

        /**
         * Add a packet to the flow
         * 
         * @param packet   Packet to add
         * @param ts       Timestamp of the packet
         * @param packetId Unique identifier for the packet
         */
        public void addPacket(Packet packet, long ts, String packetId) {
                packets.add(packet);
                packetTimestamps.add(ts);
                packetList.add(packet.getRawData());
                bytes += packet.getRawData().length;
                packetCount++;

                packetSummaries.add(packetId);

        }

        /**
         * Get the list of packets in the flow
         * 
         * @return List of packets
         */
        public List<Packet> getPackets() {
                return packets;
        }

        /**
         * Get the list of packet summaries in the flow
         * 
         * @return List of packet summaries
         */
        public List<String> getPacketSummaries() {
                return packetSummaries;
        }

        /**
         * Get the packet summaries as a single string
         * 
         * @return Comma-separated string of packet summaries
         */
        public String getPacketSummariesString() {
                return String.join(",", packetSummaries);
        }

        /**
         * Get the list of packet byte arrays in the flow
         * 
         * @return List of byte arrays
         */
        public List<byte[]> getPacketsByte() {
                return packetList;
        }

        /**
         * Calculate various statistics for the flow
         * Computes metrics like packet lengths, rates, upstream/downstream stats,
         * inter-arrival times, protocol fractions, and TCP flag rates.
         */
        public void calculateStats() {
                /* ----------------------------- Security check ----------------------------- */
                if (packets.isEmpty() || packetTimestamps.isEmpty())
                        return;
                if (packets.size() != packetTimestamps.size()) {
                        logger.error("Flow.calculateStats: packets.size() != packetTimestamps.size() ({} != {})",
                                        packets.size(), packetTimestamps.size());
                        return;
                }
                /* -------------------------- Reset stats variables ------------------------- */
                resetStats();

                /* ---------------------------------- Vars ---------------------------------- */
                List<Integer> packetLengths = new ArrayList<>();
                List<Long> interArrivals = new ArrayList<>();
                this.bytes = 0;

                // Up/Down counters
                long upstreamBytes = 0, downstreamBytes = 0;
                long upstreamPackets = 0, downstreamPackets = 0;
                // Vérifie si les timestamps sont croissants
                boolean sorted = true;
                for (int i = 1; i < packetTimestamps.size(); i++) {
                        if (packetTimestamps.get(i) < packetTimestamps.get(i - 1)) {
                                sorted = false;
                                break;
                        }
                }

                // Si pas trié, on trie les paquets et timestamps
                if (!sorted) {
                        List<Integer> indices = new ArrayList<>();
                        for (int i = 0; i < packetTimestamps.size(); i++) {
                                indices.add(i);
                        }

                        // Trie les indices selon les timestamps
                        indices.sort(Comparator.comparingLong(packetTimestamps::get));

                        // Crée des nouvelles listes triées
                        CopyOnWriteArrayList<Packet> sortedPackets = new CopyOnWriteArrayList<>();
                        CopyOnWriteArrayList<Long> sortedTimestamps = new CopyOnWriteArrayList<>();

                        for (int idx : indices) {
                                sortedPackets.add(packets.get(idx));
                                sortedTimestamps.add(packetTimestamps.get(idx));
                        }

                        // Remplace les listes originales
                        packets = sortedPackets;
                        packetTimestamps = sortedTimestamps;
                }

                /* -------------------------------- Main loop ------------------------------- */
                for (int i = 0; i < packets.size(); i++) {
                        Packet pkt = packets.get(i);
                        long ts = packetTimestamps.get(i);

                        // --- Longueur totale du paquet (inclut tous les headers et payload) ---
                        int pktLength = pkt.length();
                        packetLengths.add(pktLength);
                        this.bytes += pktLength; // total bytes sur le flow

                        // --- Direction du paquet (upstream / downstream) ---
                        if (pkt.contains(IpPacket.class)) {
                                IpPacket ip = pkt.get(IpPacket.class);

                                String srcAddr = ip.getHeader().getSrcAddr().getHostAddress();
                                String dstAddr = ip.getHeader().getDstAddr().getHostAddress();

                                if (srcAddr.equals(this.srcIp)) {
                                        upstreamBytes += pktLength; // paquet entier
                                        upstreamPackets++;
                                } else if (dstAddr.equals(this.srcIp)) {
                                        downstreamBytes += pktLength; // paquet entier
                                        downstreamPackets++;
                                } else {
                                        logger.warn("Flow.calculateStats: Packet IPs do not match flow IPs (src: {}, dst: {})",
                                                        srcAddr, dstAddr);
                                }
                        } else {
                                // Paquet non-IP : on peut juste l'ignorer pour upstream/downstream
                                logger.debug("Non-IP packet ignored for upstream/downstream calculation: length={}",
                                                pktLength);
                        }

                        // Protocol fractions and TCP flags
                        if (pkt.contains(TcpPacket.class)) {
                                tcpCount++;
                                TcpPacket tcp = pkt.get(TcpPacket.class);
                                TcpPacket.TcpHeader hdr = tcp.getHeader();
                                if (hdr.getSyn())
                                        synCount++;
                                if (hdr.getFin())
                                        finCount++;
                                if (hdr.getRst())
                                        rstCount++;
                                if (hdr.getAck())
                                        ackCount++;
                                if (hdr.getPsh())
                                        pshCount++;
                        } else if (pkt.contains(UdpPacket.class)) {
                                udpCount++;
                        } else {
                                otherCount++;
                        }

                        // Inter-arrival
                        if (i > 0) {
                                long delta = ts - packetTimestamps.get(i - 1);
                                interArrivals.add(delta);
                        }
                }

                /* ------------------------------- Basic stats ------------------------------ */
                this.packetCount = packets.size();
                this.firstSeen = packetTimestamps.get(0);
                this.lastSeen = packetTimestamps.get(packetTimestamps.size() - 1);
                // If inverted timestamps invert
                if (this.firstSeen > this.lastSeen) {
                        long temp = this.firstSeen;
                        this.firstSeen = this.lastSeen;
                        this.lastSeen = temp;
                }
                this.flowDurationMs = Math.max(1, lastSeen - firstSeen);

                /* ------------------------ Stats packet length stats ----------------------- */
                this.minPacketLength = packetLengths.stream().mapToInt(v -> v).min().orElse(0);
                this.maxPacketLength = packetLengths.stream().mapToInt(v -> v).max().orElse(0);
                this.meanPacketLength = (double) packetLengths.stream().mapToInt(v -> v).average().orElse(0);
                int n = packetLengths.size();
                double mean = packetLengths.stream().mapToInt(v -> v).average().orElse(0.0);

                double variance = packetLengths.stream()
                                .mapToDouble(v -> Math.pow(v - mean, 2))
                                .sum() / (n - 1); // correction de Bessel
                this.stddevPacketLength = Math.sqrt(variance);

                /* ------------------------------- Stats débit ------------------------------ */
                if (firstSeen == lastSeen) {
                        this.packetsPerSecond = 0;
                        this.bytesPerSecond = 0;
                } else {
                        this.bytesPerSecond = (double) ((bytes * 1000) / flowDurationMs);
                        this.packetsPerSecond = (double) ((packetCount * 1000) / flowDurationMs);
                }

                /* -------------------------- Upstream / Downstream ------------------------- */
                this.totalBytesUpstream = upstreamBytes;
                this.totalBytesDownstream = downstreamBytes;
                this.totalPacketsUpstream = upstreamPackets;
                this.totalPacketsDownstream = downstreamPackets;

                this.ratioBytesUpstreamDownstream = downstreamBytes > 0
                                ? (double) upstreamBytes / downstreamBytes
                                : 1.0;
                this.ratioPacketsUpstreamDownstream = downstreamPackets > 0
                                ? (double) upstreamPackets / downstreamPackets
                                : 1.0;

                /* ------------------------------ Flow symmetry ----------------------------- */
                this.flowSymmetry = (upstreamBytes + downstreamBytes > 0)
                                ? (double) Math.min(upstreamBytes, downstreamBytes)
                                                / Math.max(upstreamBytes, downstreamBytes)
                                : 1.0;

                /* --------------------------- Inter-arrival times -------------------------- */
                if (!interArrivals.isEmpty()) {
                        this.interArrivalTimeMin = interArrivals.stream().mapToLong(v -> v).min().orElse(0);
                        this.interArrivalTimeMax = interArrivals.stream().mapToLong(v -> v).max().orElse(0);
                        int n2 = interArrivals.size();
                        double meanIAT = interArrivals.stream().mapToLong(v -> v).average().orElse(0.0);
                        this.interArrivalTimeMean = meanIAT;

                        double variance2 = interArrivals.stream()
                                        .mapToDouble(v -> Math.pow(v - meanIAT, 2))
                                        .sum() / (n2 - 1); // Correction de Bessel
                        this.interArrivalTimeStdDev = Math.sqrt(variance2);

                }

                /* -------------------------- Protocols repartition ------------------------- */
                if (packetCount > 0) {
                        this.tcpFraction = (double) tcpCount / packetCount;
                        this.udpFraction = (double) udpCount / packetCount;
                        this.otherFraction = (double) otherCount / packetCount;
                } else {
                        this.tcpFraction = 0.0;
                        this.udpFraction = 0.0;
                        this.otherFraction = 0.0;
                }

                /* -------------------------- TCP Flags repartition ------------------------- */
                if (tcpCount > 0) {
                        this.synRate = (double) synCount / tcpCount;
                        this.finRate = (double) finCount / tcpCount;
                        this.rstRate = (double) rstCount / tcpCount;
                        this.ackRate = (double) ackCount / tcpCount;
                        this.pshRate = (double) pshCount / tcpCount;
                }

                /* ------------------- Additional stats can be added here ------------------- */

        }

        private void resetStats() {
                this.firstSeen = -1;
                this.lastSeen = -1;
                this.bytes = -1;
                this.packetCount = -1;

                this.minPacketLength = -1;
                this.maxPacketLength = -1;
                this.meanPacketLength = -1;
                this.stddevPacketLength = -1;

                this.bytesPerSecond = -1;
                this.packetsPerSecond = -1;

                this.totalBytesUpstream = -1;
                this.totalBytesDownstream = -1;
                this.totalPacketsUpstream = -1;
                this.totalPacketsDownstream = -1;
                this.ratioBytesUpstreamDownstream = -1;
                this.ratioPacketsUpstreamDownstream = -1;

                this.flowDurationMs = -1;

                this.interArrivalTimeMean = 0;
                this.interArrivalTimeStdDev = 0;
                this.interArrivalTimeMin = 0;
                this.interArrivalTimeMax = 0;

                this.flowSymmetry = 0;
                this.synRate = 0;
                this.finRate = 0;
                this.rstRate = 0;
                this.ackRate = 0;

                this.synCount = 0;
                this.finCount = 0;
                this.rstCount = 0;
                this.ackCount = 0;
                this.pshCount = 0;

                this.tcpFraction = -1;
                this.udpFraction = -1;
                this.otherFraction = -1;

        }

        /**
         * Display flow parameters in logs
         */
        public void display() {
                logger.info(">> Flow parameters:");
                logger.info("srcIp: {}", srcIp);
                logger.info("dstIp: {}", dstIp);
                logger.info("srcPort: {}", srcPort);
                logger.info("dstPort: {}", dstPort);
                logger.info("protocol: {}", protocol);
                logger.info("key: {}", key);
                logger.info("firstSeen: {}", firstSeen);
                logger.info("lastSeen: {}", lastSeen);
                logger.info("bytes: {}", bytes);
                logger.info("packetCount: {}", packetCount);
                logger.info("minPacketLength: {}", minPacketLength);
                logger.info("maxPacketLength: {}", maxPacketLength);
                logger.info("meanPacketLength: {}", meanPacketLength);
                logger.info("stddevPacketLength: {}", stddevPacketLength);
                logger.info("bytesPerSecond: {}", bytesPerSecond);
                logger.info("packetsPerSecond: {}", packetsPerSecond);
                logger.info("totalBytesUpstream: {}", totalBytesUpstream);
                logger.info("totalBytesDownstream: {}", totalBytesDownstream);
                logger.info("totalPacketsUpstream: {}", totalPacketsUpstream);
                logger.info("totalPacketsDownstream: {}", totalPacketsDownstream);
                logger.info("ratioBytesUpstreamDownstream: {}", ratioBytesUpstreamDownstream);
                logger.info("ratioPacketsUpstreamDownstream: {}", ratioPacketsUpstreamDownstream);
                logger.info("flowDurationMs: {}", flowDurationMs);
                logger.info("interArrivalTimeMean: {}", interArrivalTimeMean);
                logger.info("interArrivalTimeStdDev: {}", interArrivalTimeStdDev);
                logger.info("interArrivalTimeMin: {}", interArrivalTimeMin);
                logger.info("interArrivalTimeMax: {}", interArrivalTimeMax);
                logger.info("flowSymmetry: {}", flowSymmetry);
                logger.info("synRate: {}", synRate);
                logger.info("finRate: {}", finRate);
                logger.info("rstRate: {}", rstRate);
                logger.info("ackRate: {}", ackRate);
                logger.info("tcpFraction: {}", tcpFraction);
                logger.info("udpFraction: {}", udpFraction);
                logger.info("otherFraction: {}", otherFraction);
                logger.info("ndpiFlowPtr: {}", ndpiFlowPtr);
                logger.info("appProtocol: {}", appProtocol);
                logger.info("riskLevel: {}", riskLevel);
                logger.info("riskMask: {}", riskMask);
                logger.info("riskLabel: {}", riskLabel);
                logger.info("riskSeverity: {}", riskSeverity);
                logger.info("packetSummariesString: {}", getPacketSummariesString());
                logger.info("reasonOfFlowEnd: {}", reasonOfFlowEnd);
                logger.info("treatmentDelay: {}", treatmentDelay);

        }

        // static caches for enrichment
        private static final ConcurrentHashMap<String, String> geoCache = new ConcurrentHashMap<>();
        private static final ConcurrentHashMap<String, String> dnsCache = new ConcurrentHashMap<>();
        private static final ConcurrentHashMap<String, String> whoisCache = new ConcurrentHashMap<>();

        /**
         * Enrich flow with GeoIP, DNS, and WHOIS data
         * 
         * @param geoIPService // GeoIP service for IP to country lookup
         * @param dnsService   // DNS service for reverse DNS lookup
         * @param whoisService // WHOIS service for IP to organization lookup
         * @param vertx        // Vertx instance for async operations
         * @return Future<Flow>
         */
        public Future<Flow> enrich(
                GeoIPService geoIPService,
                DnsService dnsService,
                WhoisService whoisService,
                Vertx vertx
        ) {
                Promise<Flow> promise = Promise.promise();
                logger.debug("Enriching flow: srcIp={}, dstIp={}", srcIp, dstIp);

                CompositeFuture
                        .all(
                                lookupGeo(geoIPService, vertx, srcIp),
                                lookupGeo(geoIPService, vertx, dstIp),
                                lookupDns(dnsService, vertx, srcIp),
                                lookupDns(dnsService, vertx, dstIp),
                                lookupWhois(whoisService, vertx, srcIp),
                                lookupWhois(whoisService, vertx, dstIp)
                        )
                        .onSuccess(cf -> {
                                this.srcCountry = cf.resultAt(0);
                                this.dstCountry = cf.resultAt(1);
                                this.srcDomain = cf.resultAt(2);
                                this.dstDomain = cf.resultAt(3);
                                this.srcOrg = cf.resultAt(4);
                                this.dstOrg = cf.resultAt(5);

                                promise.complete(this);
                        })
                        .onFailure(err -> {
                                logger.warn("Enrichment failed for flow {}: {}", key, err.getMessage());
                                promise.complete(this); // fallback : compléter malgré erreur
                        });

                return promise.future();
        }

        // Méthodes auxiliaires privées
        private Future<String> lookupGeo(GeoIPService service, Vertx vertx, String ip) {
                logger.debug("Looking up GeoIP for IP: {}", ip);
                if (geoCache.containsKey(ip))
                        return Future.succeededFuture(geoCache.get(ip));
                Promise<String> p = Promise.promise();
                vertx.executeBlocking(fut -> {
                        try {
                                String country = service.getCountryByIP(ip);
                                geoCache.put(ip, country);
                                logger.debug("GeoIP lookup result for IP {}: {}", ip, country);
                                fut.complete(country);
                        } catch (Exception e) {
                                logger.debug("Error during GeoIP lookup for IP {}: {}", ip, e.getMessage());
                                fut.complete("N/A");
                        }
                }, false, p);
                return p.future().recover(err -> Future.succeededFuture("N/A"));
        }

        private Future<String> lookupDns(DnsService service, Vertx vertx, String ip) {

                logger.debug("Looking up DNS for IP: {}", ip);
                if (dnsCache.containsKey(ip))
                        return Future.succeededFuture(dnsCache.get(ip));
                Promise<String> p = Promise.promise();
                vertx.executeBlocking(fut -> {
                        try {
                                String host = service.reverseLookupBlocking(ip);
                                dnsCache.put(ip, host);
                                logger.debug("DNS lookup result for IP {}: {}", ip, host);
                                fut.complete(host);
                        } catch (Exception e) {
                                logger.debug("Error during DNS lookup for IP {}: {}", ip, e.getMessage());
                                fut.complete("N/A");
                        }
                }, false, p);
                return p.future().recover(err -> Future.succeededFuture("N/A"));
        }

        private Future<String> lookupWhois(WhoisService service, Vertx vertx, String ip) {
                logger.debug("Looking up WHOIS for IP: {}", ip);
                if (whoisCache.containsKey(ip))
                        return Future.succeededFuture(whoisCache.get(ip));
                Promise<String> p = Promise.promise();
                vertx.executeBlocking(fut -> {
                        try {
                                String org = service.lookupBlocking(ip);
                                logger.debug("WHOIS lookup result for IP {}: {}", ip, org);
                                org = parseWhoisOrg(org);
                                if (org == null || org.isEmpty()) {
                                        whoisCache.put(ip, "N/A");
                                        fut.complete("N/A");
                                } else if (org.contains("No match")) {
                                        GeoIPService geoIPService = new GeoIPService(
                                                        "src/main/resources/GeoLite2-City.mmdb",
                                                        "src/main/resources/GeoLite2-ASN.mmdb");
                                        String asnOrg = geoIPService.getOrgByIP(ip);
                                        whoisCache.put(ip, asnOrg);
                                        fut.complete(asnOrg);
                                } else {
                                        whoisCache.put(ip, org);
                                        fut.complete(org);
                                }
                        } catch (Exception e) {
                                logger.debug("Error during WHOIS lookup for IP {}: {}", ip, e.getMessage());
                                fut.complete("N/A");
                        }
                }, false, p);
                return p.future().recover(err -> Future.succeededFuture("N/A"));
        }

        /**
         * Parse the organization name from the raw WHOIS response.
         * 
         * @param whoisRaw The raw WHOIS response.
         * @return The organization name or "No match" if not found.
         */
        private String parseWhoisOrg(String whoisRaw) {
                if (whoisRaw == null)
                        return "N/A";
                String[] lines = whoisRaw.split("\n");
                for (String line : lines) {
                        line = line.trim();
                        if (line.startsWith("OrgName:") || line.startsWith("Organization:")) {
                                return line.split(":", 2)[1].trim();
                        }
                }
                return "No match";
        }

        public boolean isProperlyClosed() {
                return finFromSrc && finAckedByDst && finFromDst && finAckedBySrc;
        }

}
