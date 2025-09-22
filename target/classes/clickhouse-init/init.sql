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

CREATE USER IF NOT EXISTS admin IDENTIFIED WITH plaintext_password BY 'admin';
GRANT ALL ON network_analysis.* TO admin;
GRANT ALL ON network_analysis.network_data TO admin;

