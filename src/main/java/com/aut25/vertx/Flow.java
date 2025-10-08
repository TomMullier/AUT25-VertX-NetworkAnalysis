package com.aut25.vertx;

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
import java.util.ArrayList;
import java.util.List;

/**
 * Internal Flow class to hold flow state
 * Represents a network flow with its attributes and timestamps.
 */
public class Flow {
        final String srcIp;
        final String dstIp;
        final Integer srcPort;
        final Integer dstPort;
        final String protocol;
        final String key;

        long firstSeen;
        long lastSeen;
        long bytes = -1;
        long packetCount = -1;

        // Stats of flow
        double minPacketLength = -1;
        double maxPacketLength = -1;
        double meanPacketLength = -1;
        double stddevPacketLength = -1;

        double bytesPerSecond = -1;
        double packetsPerSecond = -1;

        double totalBytesUpstream = -1;
        double totalBytesDownstream = -1;
        double totalPacketsUpstream = -1;
        double totalPacketsDownstream = -1;
        double ratioBytesUpstreamDownstream = -1;
        double ratioPacketsUpstreamDownstream = -1;

        double flowDurationMs = -1;

        double interArrivalTimeMean = -1;
        double interArrivalTimeStdDev = -1;
        double interArrivalTimeMin = -1;
        double interArrivalTimeMax = -1;

        // IDS-friendly features
        double flowSymmetry = -1;
        double synRate = -1;
        double finRate = -1;
        double rstRate = -1;
        double ackRate = -1;

        double tcpFraction = -1;
        double udpFraction = -1;
        double otherFraction = -1;

        // List of packets for this flow
        List<Packet> packets = new ArrayList<>();
        List<Long> packetTimestamps = new ArrayList<>();

        public long ndpiFlowPtr = 0;
        private final List<byte[]> packetList = new ArrayList<>();
        public String appProtocol;
        public byte[] appProtocolBytes;

        public int riskLevel = -1;
        public int riskMask = -1;
        public String riskLabel = "UNKNOWN";
        public String riskSeverity = "UNKNOWN";

        Logger logger = LoggerFactory.getLogger(Flow.class);

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
        JsonObject getJsonObject() {
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

                return jo;
        }

        /**
         * Add a packet to the flow
         * 
         * @param packet Packet to add
         * @param ts     Timestamp of the packet
         */
        public void addPacket(Packet packet, long ts) {
                packets.add(packet);
                packetTimestamps.add(ts);
                packetList.add(packet.getRawData());
                bytes += packet.getRawData().length;
                packetCount++;

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
                if (packets.isEmpty() || packetTimestamps.isEmpty())
                        return;

                this.packetCount = packets.size();
                this.bytes = 0;
                List<Integer> packetLengths = new ArrayList<>();
                List<Long> interArrivals = new ArrayList<>();

                // Compteurs Up/Down
                long upstreamBytes = 0, downstreamBytes = 0;
                long upstreamPackets = 0, downstreamPackets = 0;

                // Flags TCP
                long synCount = 0, finCount = 0, rstCount = 0, ackCount = 0;
                long tcpCount = 0, udpCount = 0, otherCount = 0;

                // Durée du flux
                this.firstSeen = packetTimestamps.get(0);
                this.lastSeen = packetTimestamps.get(packetTimestamps.size() - 1);
                this.flowDurationMs = Math.max(1, lastSeen - firstSeen);

                // Boucle principale
                for (int i = 0; i < packets.size(); i++) {
                        Packet pkt = packets.get(i);
                        long ts = packetTimestamps.get(i);

                        int length = pkt.length();
                        packetLengths.add(length);
                        this.bytes += length;

                        // Détermination direction (Upstream = srcIp, Downstream = dstIp)
                        if (pkt.contains(IpPacket.class)) {
                                IpPacket ip = pkt.get(IpPacket.class);
                                int payloadLength = ip.getPayload() != null ? ip.getPayload().length() : 0;

                                if (ip.getHeader().getSrcAddr().getHostAddress().equals(this.srcIp)) {
                                        upstreamBytes += payloadLength;
                                        upstreamPackets++;
                                } else {
                                        downstreamBytes += payloadLength;
                                        downstreamPackets++;
                                }
                        }

                        // Analyse protocole
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

                // === Stats packets ===
                this.minPacketLength = packetLengths.stream().mapToInt(v -> v).min().orElse(0);
                this.maxPacketLength = packetLengths.stream().mapToInt(v -> v).max().orElse(0);
                this.meanPacketLength = (long) packetLengths.stream().mapToInt(v -> v).average().orElse(0);

                double mean = this.meanPacketLength;
                this.stddevPacketLength = (long) Math.sqrt(packetLengths.stream()
                                .mapToDouble(v -> Math.pow(v - mean, 2)).average().orElse(0));

                // === Stats débit ===
                this.bytesPerSecond = (long) ((bytes * 1000.0) / flowDurationMs);
                this.packetsPerSecond = (long) ((packetCount * 1000.0) / flowDurationMs);

                // === Upstream / Downstream ===
                this.totalBytesUpstream = upstreamBytes;
                this.totalBytesDownstream = downstreamBytes;
                this.totalPacketsUpstream = upstreamPackets;
                this.totalPacketsDownstream = downstreamPackets;

                this.ratioBytesUpstreamDownstream = (downstreamBytes > 0)
                                ? (long) ((double) upstreamBytes / downstreamBytes)
                                : 1;

                this.ratioPacketsUpstreamDownstream = (downstreamPackets > 0)
                                ? (long) ((double) upstreamPackets / downstreamPackets)
                                : 1;

                // Symétrie du flux (proche de 1 = équilibré)
                this.flowSymmetry = (upstreamBytes + downstreamBytes > 0)
                                ? (double) Math.min(upstreamBytes, downstreamBytes)
                                                / Math.max(upstreamBytes, downstreamBytes)
                                : 0;

                // === Inter-arrival times ===
                if (!interArrivals.isEmpty()) {
                        this.interArrivalTimeMin = interArrivals.stream().mapToLong(v -> v).min().orElse(0);
                        this.interArrivalTimeMax = interArrivals.stream().mapToLong(v -> v).max().orElse(0);
                        this.interArrivalTimeMean = (long) interArrivals.stream().mapToLong(v -> v).average().orElse(0);

                        double meanIAT = this.interArrivalTimeMean;
                        this.interArrivalTimeStdDev = (long) Math.sqrt(interArrivals.stream()
                                        .mapToDouble(v -> Math.pow(v - meanIAT, 2)).average().orElse(0));
                }

                // === Fractions protocoles ===
                this.tcpFraction = (double) tcpCount / packetCount;
                this.udpFraction = (double) udpCount / packetCount;
                this.otherFraction = (double) otherCount / packetCount;

                // === TCP Flags rates ===
                if (tcpCount > 0) {
                        this.synRate = (double) synCount / tcpCount;
                        this.finRate = (double) finCount / tcpCount;
                        this.rstRate = (double) rstCount / tcpCount;
                        this.ackRate = (double) ackCount / tcpCount;
                }

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

        }

}
