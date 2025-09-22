-- Création de la base
CREATE DATABASE IF NOT EXISTS network_analysis;

-- Création de la table pour stocker les paquets
CREATE TABLE IF NOT EXISTS network_analysis.packets
(
    timestamp   DateTime,
    srcIp       String,
    dstIp       String,
    protocol    String,
    bytes       UInt32,
    rawPacket   String
)
ENGINE = MergeTree
ORDER BY (timestamp);
