CREATE DATABASE IF NOT EXISTS network_analysis;

USE network_analysis;

DROP TABLE IF EXISTS network_analysis.network_data;

CREATE TABLE IF NOT EXISTS network_analysis.network_data (
        id Nullable(String),
            timestamp DateTime64(3, 'UTC') DEFAULT now64(3),
            srcIp Nullable(String),
            dstIp Nullable(String),
            protocol Nullable(String),
            bytes Nullable(UInt32),
            rawPacket Nullable(String)
        ) ENGINE = MergeTree()
ORDER BY timestamp;

DROP TABLE IF EXISTS network_analysis.network_flows;

CREATE TABLE IF NOT EXISTS network_analysis.network_flows (
    id Nullable(String),
    flowKey String,
    firstSeen DateTime64(3, 'UTC'),
    lastSeen DateTime64(3, 'UTC'),
    srcIp Nullable(String),
    dstIp Nullable(String),
    srcPort Nullable(String),
    dstPort Nullable(String),
    protocol Nullable(String),
    bytes Nullable(UInt64),
    packets Nullable(UInt64),
    packetSummaries Array(String),
    durationMs Nullable(UInt64),
    reasonOfFlowEnd Nullable(String),
    minPacketLength Nullable(UInt32),
    maxPacketLength Nullable(UInt32),
    meanPacketLength Nullable(Float32),
    stddevPacketLength Nullable(Float32),
    bytesPerSecond Nullable(Float32),
    packetsPerSecond Nullable(Float32),
    totalBytesUpstream Nullable(Float32),
    totalBytesDownstream Nullable(Float32),
    totalPacketsUpstream Nullable(Float32),
    totalPacketsDownstream Nullable(Float32),
    ratioBytesUpDown Nullable(Float32),
    ratioPacketsUpDown Nullable(Float32),
    flowDurationMs Nullable(Float32),
    interArrivalTimeMean Nullable(Float32),
    interArrivalTimeStdDev Nullable(Float32),
    interArrivalTimeMin Nullable(Float32),
    interArrivalTimeMax Nullable(Float32),
    flowSymmetry Nullable(Float32),
    synRate Nullable(Float32),
    finRate Nullable(Float32),
    rstRate Nullable(Float32),
    ackRate Nullable(Float32),
    tcpFraction Nullable(Float32),
    udpFraction Nullable(Float32),
    otherFraction Nullable(Float32),
    appProtocolBytes Nullable(Float32),
    appProtocol Nullable(String),
    riskLevel Nullable(Int32),
    riskMask Nullable(Int32),
    riskLabel Array(String),
    riskSeverity Nullable(String),
    ndpiFlowPtr Nullable(UInt64),
    srcCountry Nullable(String),
    dstCountry Nullable(String),
    srcDomain Nullable(String),
    dstDomain Nullable(String),
    srcOrg Nullable(String),
    dstOrg Nullable(String)
) ENGINE = MergeTree()
ORDER BY (firstSeen, flowKey);

CREATE USER IF NOT EXISTS admin IDENTIFIED WITH plaintext_password BY 'admin';
GRANT ALL ON network_analysis.* TO admin;
GRANT ALL ON network_analysis.network_data TO admin;

