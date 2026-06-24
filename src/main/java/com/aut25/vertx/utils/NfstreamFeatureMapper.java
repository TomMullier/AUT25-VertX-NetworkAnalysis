package com.aut25.vertx.utils;


// ======================= All this file is new added by me 
import java.util.HashMap;
import java.util.Map;

public class NfstreamFeatureMapper {

    public static Map<String, Object> toNfstreamMap(Flow f) {
        Map<String, Object> m = new HashMap<>();

        // Identity / routing fields (model may use these or may not — include them)
        m.put("id",                      0L);             // no equivalent, set 0
        m.put("expiration_id",           0L);             // 0=idle, 1=active, 2=rst/fin — use 0
        // These 4 were missing — add them
        m.put("protocol",   protocolToNumber(f.protocol));
        m.put("ip_version", f.srcIp != null && f.srcIp.contains(":") ? 6.0 : 4.0);
        m.put("vlan_id",    0.0);   // no VLAN tracking — safe default
        m.put("tunnel_id",  0.0);   // no tunnel tracking — safe default
        m.put("src_port", f.srcPort != null ? (double) f.srcPort : 0.0);
        m.put("dst_port", f.dstPort != null ? (double) f.dstPort : 0.0);

        // Bidirectional totals
        m.put("bidirectional_first_seen_ms", (double) f.firstSeen);
        m.put("bidirectional_last_seen_ms",  (double) f.lastSeen);
        m.put("bidirectional_duration_ms",   (double) f.flowDurationMs);
        m.put("bidirectional_packets",       (double) f.packetCount);
        m.put("bidirectional_bytes",         (double) f.bytes);

        // src→dst direction
        m.put("src2dst_first_seen_ms",  f.src2dstTimestamps.isEmpty() ? 0.0 : (double) f.src2dstTimestamps.get(0));
        m.put("src2dst_last_seen_ms",   f.src2dstTimestamps.isEmpty() ? 0.0 : (double) f.src2dstTimestamps.get(f.src2dstTimestamps.size()-1));
        m.put("src2dst_duration_ms",    f.src2dstTimestamps.size() < 2 ? 0.0
                                            : (double)(f.src2dstTimestamps.get(f.src2dstTimestamps.size()-1) - f.src2dstTimestamps.get(0)));
        m.put("src2dst_packets",        (double) f.src2dstPackets);
        m.put("src2dst_bytes",          (double) f.src2dstBytes);

        // dst→src direction
        m.put("dst2src_first_seen_ms",  f.dst2srcTimestamps.isEmpty() ? 0.0 : (double) f.dst2srcTimestamps.get(0));
        m.put("dst2src_last_seen_ms",   f.dst2srcTimestamps.isEmpty() ? 0.0 : (double) f.dst2srcTimestamps.get(f.dst2srcTimestamps.size()-1));
        m.put("dst2src_duration_ms",    f.dst2srcTimestamps.size() < 2 ? 0.0
                                            : (double)(f.dst2srcTimestamps.get(f.dst2srcTimestamps.size()-1) - f.dst2srcTimestamps.get(0)));
        m.put("dst2src_packets",        (double) f.dst2srcPackets);
        m.put("dst2src_bytes",          (double) f.dst2srcBytes);

        // Bidirectional packet size stats
        m.put("bidirectional_min_ps",   f.minPacketLength);
        m.put("bidirectional_mean_ps",  f.meanPacketLength);
        m.put("bidirectional_stddev_ps",f.stddevPacketLength);
        m.put("bidirectional_max_ps",   f.maxPacketLength);

        // Per-direction packet size stats
        m.put("src2dst_min_ps",         f.src2dstMinPs);
        m.put("src2dst_mean_ps",        f.src2dstMeanPs);
        m.put("src2dst_stddev_ps",      f.src2dstStdPs);
        m.put("src2dst_max_ps",         f.src2dstMaxPs);

        m.put("dst2src_min_ps",         f.dst2srcMinPs);
        m.put("dst2src_mean_ps",        f.dst2srcMeanPs);
        m.put("dst2src_stddev_ps",      f.dst2srcStdPs);
        m.put("dst2src_max_ps",         f.dst2srcMaxPs);

        // Bidirectional inter-arrival times
        m.put("bidirectional_min_piat_ms",    f.interArrivalTimeMin);
        m.put("bidirectional_mean_piat_ms",   f.interArrivalTimeMean);
        m.put("bidirectional_stddev_piat_ms", f.interArrivalTimeStdDev);
        m.put("bidirectional_max_piat_ms",    f.interArrivalTimeMax);

        // Per-direction inter-arrival times
        m.put("src2dst_min_piat_ms",    f.src2dstMinPiatMs);
        m.put("src2dst_mean_piat_ms",   f.src2dstMeanPiatMs);
        m.put("src2dst_stddev_piat_ms", f.src2dstStdPiatMs);
        m.put("src2dst_max_piat_ms",    f.src2dstMaxPiatMs);

        m.put("dst2src_min_piat_ms",    f.dst2srcMinPiatMs);
        m.put("dst2src_mean_piat_ms",   f.dst2srcMeanPiatMs);
        m.put("dst2src_stddev_piat_ms", f.dst2srcStdPiatMs);
        m.put("dst2src_max_piat_ms",    f.dst2srcMaxPiatMs);

        // TCP flags — bidirectional
        m.put("bidirectional_syn_packets", (double)(f.src2dstSynCount + f.dst2srcSynCount));
        m.put("bidirectional_cwr_packets", 0.0);   // not tracked — safe default
        m.put("bidirectional_ece_packets", 0.0);
        m.put("bidirectional_urg_packets", 0.0);
        m.put("bidirectional_ack_packets", (double)(f.src2dstAckCount + f.dst2srcAckCount));
        m.put("bidirectional_psh_packets", (double)(f.src2dstPshCount + f.dst2srcPshCount));
        m.put("bidirectional_rst_packets", (double)(f.src2dstRstCount + f.dst2srcRstCount));
        m.put("bidirectional_fin_packets", (double)(f.src2dstFinCount + f.dst2srcFinCount));

        // TCP flags — src→dst
        m.put("src2dst_syn_packets", (double) f.src2dstSynCount);
        m.put("src2dst_cwr_packets", 0.0);
        m.put("src2dst_ece_packets", 0.0);
        m.put("src2dst_urg_packets", (double) f.src2dstUrgCount);
        m.put("src2dst_ack_packets", (double) f.src2dstAckCount);
        m.put("src2dst_psh_packets", (double) f.src2dstPshCount);
        m.put("src2dst_rst_packets", (double) f.src2dstRstCount);
        m.put("src2dst_fin_packets", (double) f.src2dstFinCount);

        // TCP flags — dst→src
        m.put("dst2src_syn_packets", (double) f.dst2srcSynCount);
        m.put("dst2src_cwr_packets", 0.0);
        m.put("dst2src_ece_packets", 0.0);
        m.put("dst2src_urg_packets", (double) f.dst2srcUrgCount);
        m.put("dst2src_ack_packets", (double) f.dst2srcAckCount);
        m.put("dst2src_psh_packets", (double) f.dst2srcPshCount);
        m.put("dst2src_rst_packets", (double) f.dst2srcRstCount);
        m.put("dst2src_fin_packets", (double) f.dst2srcFinCount);

        // Application layer — nDPI fields your Java side can't replicate
        // Set safe neutral values; they'll contribute little once all real features are populated
        m.put("application_name",        "Unknown");
        m.put("application_category_name","Unspecified");
        m.put("application_is_guessed",  0.0);
        m.put("application_confidence",  0.0);
        m.put("requested_server_name",   "");
        m.put("client_fingerprint",      "");
        m.put("server_fingerprint",      "");

        return m;
    }
    private static double protocolToNumber(String protocol) {
    if (protocol == null) return 0.0;
    switch (protocol.toUpperCase()) {
        case "TCP":    return 6.0;
        case "UDP":    return 17.0;
        case "ICMP":   return 1.0;
        case "ICMPV6": return 58.0;
        case "SCTP":   return 132.0;
        default:       return 0.0;
    }
}
}