CREATE DATABASE IF NOT EXISTS network_analysis;

CREATE TABLE IF NOT EXISTS network_analysis.network_data (
    timestamp DateTime,
    rawPacket String,
    srcIp String,
    dstIp String,
    protocol String,
    bytes UInt32
) ENGINE = MergeTree()
ORDER BY timestamp;
