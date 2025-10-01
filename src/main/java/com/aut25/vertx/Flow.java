package com.aut25.vertx;

import io.vertx.core.json.JsonObject;

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
        long bytes = 0;
        long packetCount = 0;
        long lastUpdated;
        

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
                return jo;
        }
}
