package com.aut25.vertx;

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
        long firstSeen;
        long lastSeen;
        long bytes = 0;
        long packetCount = 0;
        final String key;

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
