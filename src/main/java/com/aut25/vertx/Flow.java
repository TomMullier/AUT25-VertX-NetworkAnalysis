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
        long stddevPacketLength = -1;

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

        Logger logger = LoggerFactory.getLogger(Flow.class);

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
                jo.put("durationMs", this.lastSeen - this.firstSeen);
                jo.put("flowKey", this.key);

                // Stats
                jo.put("minPacketLength", this.minPacketLength);
                jo.put("maxPacketLength", this.maxPacketLength);
                jo.put("meanPacketLength", this.meanPacketLength);
                jo.put("stddevPacketLength", this.stddevPacketLength);

                jo.put("bytesPerSecond", this.bytesPerSecond);
                jo.put("packetsPerSecond", this.packetsPerSecond);

                jo.put("totalBytesUpstream", this.totalBytesUpstream);
                jo.put("totalBytesDownstream", this.totalBytesDownstream);
                jo.put("totalPacketsUpstream", this.totalPacketsUpstream);
                jo.put("totalPacketsDownstream", this.totalPacketsDownstream);
                jo.put("ratioBytesUpDown", this.ratioBytesUpstreamDownstream);
                jo.put("ratioPacketsUpDown", this.ratioPacketsUpstreamDownstream);

                jo.put("flowDurationMs", this.flowDurationMs);

                jo.put("interArrivalTimeMean", this.interArrivalTimeMean);
                jo.put("interArrivalTimeStdDev", this.interArrivalTimeStdDev);
                jo.put("interArrivalTimeMin", this.interArrivalTimeMin);
                jo.put("interArrivalTimeMax", this.interArrivalTimeMax);

                // IDS extra
                jo.put("flowSymmetry", this.flowSymmetry);
                jo.put("synRate", this.synRate);
                jo.put("finRate", this.finRate);
                jo.put("rstRate", this.rstRate);
                jo.put("ackRate", this.ackRate);

                jo.put("tcpFraction", this.tcpFraction);
                jo.put("udpFraction", this.udpFraction);
                jo.put("otherFraction", this.otherFraction);

                return jo;
        }

        public void addPacket(Packet packet, long ts) {
                packets.add(packet);
                packetTimestamps.add(ts);
        }

        public List<Packet> getPackets() {
                return packets;
        }

        public void calculateStats() {
                if (packets.isEmpty() || packetTimestamps.isEmpty())
                        return;

                minPacketLength = packets.stream().mapToLong(Packet::length).min().orElse(-1);
                maxPacketLength = packets.stream().mapToLong(Packet::length).max().orElse(-1);
                meanPacketLength = (double) packets.stream().mapToLong(Packet::length).average().orElse(-1);
                stddevPacketLength = (long) Math.sqrt(packets.stream()
                                .mapToLong(Packet::length)
                                .mapToDouble(len -> (len - meanPacketLength) * (len - meanPacketLength))
                                .average()
                                .orElse(0));
                bytesPerSecond = (double) bytes / ((lastSeen - firstSeen) / 1000.0);
                packetsPerSecond = (double) packetCount / ((lastSeen - firstSeen) / 1000.0);

                // Upstream/Downstream calculations
                totalBytesUpstream = 0;
                totalBytesDownstream = 0;
                totalPacketsUpstream = 0;
                totalPacketsDownstream = 0;

                for (int i = 0; i < packets.size(); i++) {
                        Packet packet = packets.get(i);
                        if (packet.contains(IpPacket.class)) {
                                IpPacket ipPacket = packet.get(IpPacket.class);
                                if (ipPacket.getHeader().getSrcAddr().getHostAddress().equals(srcIp)) {
                                        totalBytesUpstream += packet.length();
                                        totalPacketsUpstream++;
                                } else if (ipPacket.getHeader().getDstAddr().getHostAddress().equals(srcIp)) {
                                        totalBytesDownstream += packet.length();
                                        totalPacketsDownstream++;
                                }
                        }
                }

                if (totalBytesDownstream > 0) {
                        ratioBytesUpstreamDownstream = totalBytesUpstream / totalBytesDownstream;
                }

                if (totalPacketsDownstream > 0) {
                        ratioPacketsUpstreamDownstream = totalPacketsUpstream / totalPacketsDownstream;
                }

                flowDurationMs = lastSeen - firstSeen;
                List<Long> interArrivalTimes = new ArrayList<>();
                for (int i = 1; i < packetTimestamps.size(); i++) {
                        interArrivalTimes.add(packetTimestamps.get(i) - packetTimestamps.get(i - 1));
                }

                interArrivalTimeMean = (long) interArrivalTimes.stream().mapToLong(Long::longValue).average().orElse(0);
                interArrivalTimeStdDev = (long) Math.sqrt(interArrivalTimes.stream()
                                .mapToLong(Long::longValue)
                                .mapToDouble(time -> (time - interArrivalTimeMean) * (time - interArrivalTimeMean))
                                .average()
                                .orElse(0.0));
                interArrivalTimeMin = interArrivalTimes.stream().mapToLong(Long::longValue).min().orElse(0);
                interArrivalTimeMax = interArrivalTimes.stream().mapToLong(Long::longValue).max().orElse(0);
                flowSymmetry = (double) Math.min(totalBytesUpstream, totalBytesDownstream) /
                                (double) Math.max(totalBytesUpstream, totalBytesDownstream);

                if (totalBytesUpstream == 0 && totalBytesDownstream == 0) {
                        flowSymmetry = 0;
                }

                // Protocol-specific calculations
                int synCount = 0;
                int finCount = 0;
                int rstCount = 0;
                int ackCount = 0;
                int tcpCount = 0;
                int udpCount = 0;
                int otherCount = 0;

                for (Packet packet : packets) {
                        if (packet.contains(TcpPacket.class)) {
                                tcpCount++;
                                TcpPacket tcpPacket = packet.get(TcpPacket.class);
                                if (tcpPacket.getHeader().getSyn()) {
                                        synCount++;
                                }
                                if (tcpPacket.getHeader().getFin()) {
                                        finCount++;
                                }
                                if (tcpPacket.getHeader().getRst()) {
                                        rstCount++;
                                }
                                if (tcpPacket.getHeader().getAck()) {
                                        ackCount++;
                                }
                        } else if (packet.contains(UdpPacket.class)) {
                                udpCount++;
                        } else {
                                otherCount++;
                        }
                }

                int totalProtocolCount = tcpCount + udpCount + otherCount;
                if (totalProtocolCount > 0) {
                        tcpFraction = (double) tcpCount / totalProtocolCount;
                        udpFraction = (double) udpCount / totalProtocolCount;
                        otherFraction = (double) otherCount / totalProtocolCount;
                }
                if (packetCount > 0) {
                        synRate = (double) synCount / packetCount;
                        finRate = (double) finCount / packetCount;
                        rstRate = (double) rstCount / packetCount;
                        ackRate = (double) ackCount / packetCount;
                }

                if (totalPacketsUpstream > 0) {

                        double avgPacketSizeUpstream = (double) totalBytesUpstream / totalPacketsUpstream;
                        logger.info("Average Packet Size Upstream: {}", avgPacketSizeUpstream);
                }

                if (totalPacketsDownstream > 0) {
                        double avgPacketSizeDownstream = (double) totalBytesDownstream / totalPacketsDownstream;
                        logger.info("Average Packet Size Downstream: {}", avgPacketSizeDownstream);
                }
        }

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
        }

}
