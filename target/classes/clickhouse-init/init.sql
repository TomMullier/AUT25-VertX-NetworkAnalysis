CREATE DATABASE IF NOT EXISTS network_analysis;

USE network_analysis;

DROP TABLE IF EXISTS network_analysis.network_data;

CREATE TABLE IF NOT EXISTS network_analysis.network_data (
        id String,
    timestamp DateTime64(3, 'UTC') DEFAULT now64(3),
    srcIp String,
    dstIp String,
    protocol String,
    bytes UInt32,
    rawPacket String
) ENGINE = MergeTree()
ORDER BY timestamp;

DROP TABLE IF EXISTS network_analysis.network_flows;

CREATE TABLE IF NOT EXISTS network_analysis.network_flows (
    id String,
    flowKey String,
    firstSeen DateTime64(3, 'UTC'),
    lastSeen DateTime64(3, 'UTC'),
    srcIp String,
    dstIp String,
    srcPort UInt64,
    dstPort UInt64,
    protocol String,
    bytes UInt64,
    packets UInt64,
    packetSummaries Array(String),
    durationMs UInt64,
    reasonOfFlowEnd String,
    minPacketLength UInt32,
    maxPacketLength UInt32,
    meanPacketLength Float32,
    stddevPacketLength Float32,
    bytesPerSecond Float32,
    packetsPerSecond Float32,
    totalBytesUpstream Float32,
    totalBytesDownstream Float32,
    totalPacketsUpstream Float32,
    totalPacketsDownstream Float32,
    ratioBytesUpDown Float32,
    ratioPacketsUpDown Float32,
    flowDurationMs Float32,
    interArrivalTimeMean Float32,
    interArrivalTimeStdDev Float32,
    interArrivalTimeMin Float32,
    interArrivalTimeMax Float32,
    flowSymmetry Float32,
    synRate Float32,
    finRate Float32,
    rstRate Float32,
    ackRate Float32,
    tcpFraction Float32,
    udpFraction Float32,
    otherFraction Float32,
    appProtocolBytes Float32,
    appProtocol String,
    riskLevel Int32,
    riskMask Int32,
    riskLabel Array(String),
    riskSeverity String,
    ndpiFlowPtr UInt64
) ENGINE = MergeTree()
ORDER BY (firstSeen, flowKey);

CREATE USER IF NOT EXISTS admin IDENTIFIED WITH plaintext_password BY 'admin';
GRANT ALL ON network_analysis.* TO admin;
GRANT ALL ON network_analysis.network_data TO admin;

