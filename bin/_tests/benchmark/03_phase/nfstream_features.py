from nfstream import NFStreamer
import csv
from datetime import datetime

# -------------------------- Paramètres -------------------------- #
# Durées en secondes
IDLE_TIMEOUT = 5  # Temps d'inactivité avant fermeture d'un flux
ACTIVE_TIMEOUT = 300  # Durée maximale de vie d'un flux

# Chemins des fichiers
PCAP_INPUT_PATH = "../000_PCAP/eth2dump-pingFloodDDoS-30m-12h_1.pcap"
CSV_OUTPUT_PATH = "csv/nfstream_features_eth2dump-pingFloodDDoS-30m-12h_1.csv"

# ----------------------- Mapping Protocoles ---------------------- #
# Dictionnaire pour convertir les numéros de protocole en noms
PROTOCOL_MAPPING = {
    0: "HOPOPT",
    1: "ICMP",
    2: "IGMP",
    3: "GGP",
    4: "IPv4",
    5: "ST",
    6: "TCP",
    7: "CBT",
    8: "EGP",
    9: "IGP",
    10: "BBN-RCC-MON",
    11: "NVP-II",
    12: "PUP",
    13: "ARGUS",
    14: "EMCON",
    15: "XNET",
    16: "CHAOS",
    17: "UDP",
    18: "MUX",
    19: "DCN-MEAS",
    20: "HMP",
    21: "PRM",
    22: "XNS-IDP",
    23: "TRUNK-1",
    24: "TRUNK-2",
    25: "LEAF-1",
    26: "LEAF-2",
    27: "RDP",
    28: "IRTP",
    29: "ISO-TP4",
    30: "NETBLT",
    31: "MFE-NSP",
    32: "MERIT-INP",
    33: "DCCP",
    34: "3PC",
    35: "IDPR",
    36: "XTP",
    37: "DDP",
    38: "IDPR-CMTP",
    39: "TP++",
    40: "IL",
    41: "IPv6",
    42: "SDRP",
    43: "IPv6-Route",
    44: "IPv6-Frag",
    45: "IDRP",
    46: "RSVP",
    47: "GRE",
    48: "DSR",
    49: "BNA",
    50: "ESP",
    51: "AH",
    52: "I-NLSP",
    53: "SWIPE",
    54: "NARP",
    55: "Min-IPv4",
    56: "TLSP",
    57: "SKIP",
    58: "IPv6-ICMP",
    59: "IPv6-NoNxt",
    60: "IPv6-Opts",
    61: "any host internal protocol",
    62: "CFTP",
    63: "any local network",
    64: "SAT-EXPAK",
    65: "KRYPTOLAN",
    66: "RVD",
    67: "IPPC",
    68: "any distributed file system",
    69: "SAT-MON",
    70: "VISA",
    71: "IPCV",
    72: "CPNX",
    73: "CPHB",
    74: "WSN",
    75: "PVP",
    76: "BR-SAT-MON",
    77: "SUN-ND",
    78: "WB-MON",
    79: "WB-EXPAK",
    80: "ISO-IP",
    81: "VMTP",
    82: "SECURE-VMTP",
    83: "VINES",
    84: "IPTM",
    85: "NSFNET-IGP",
    86: "DGP",
    87: "TCF",
    88: "EIGRP",
    89: "OSPFIGP",
    90: "Sprite-RPC",
    91: "LARP",
    92: "MTP",
    93: "AX.25",
    94: "IPIP",
    95: "MICP",
    96: "SCC-SP",
    97: "ETHERIP",
    98: "ENCAP",
    99: "any private encryption scheme",
    100: "GMTP",
    101: "IFMP",
    102: "PNNI",
    103: "PIM",
    104: "ARIS",
    105: "SCPS",
    106: "QNX",
    107: "A/N",
    108: "IPComp",
    109: "SNP",
    110: "Compaq-Peer",
    111: "IPX-in-IP",
    112: "VRRP",
    113: "PGM",
    114: "any 0-hop protocol",
    115: "L2TP",
    116: "DDX",
    117: "IATP",
    118: "STP",
    119: "SRP",
    120: "UTI",
    121: "SMP",
    122: "SM",
    123: "PTP",
    124: "ISIS over IPv4",
    125: "FIRE",
    126: "CRTP",
    127: "CRUDP",
    128: "SSCOPMCE",
    129: "IPLT",
    130: "SPS",
    131: "PIPE",
    132: "SCTP",
    133: "FC",
    134: "RSVP-E2E-IGNORE",
    135: "Mobility Header",
    136: "UDPLite",
    137: "MPLS-in-IP",
    138: "manet",
    139: "HIP",
    140: "Shim6",
    141: "WESP",
    142: "ROHC",
    143: "Ethernet",
    144: "AGGFRAG",
    145: "NSH",
    146: "Homa",
    147: "BIT-EMU",
    148: "Unassigned",
    253: "Use for experimentation",
    254: "Use for experimentation",
    255: "Reserved",
}


# ---------------------- Fonctions Utiles ------------------------- #
def timestamp_to_str(timestamp_ms):
    """
    Convertit un timestamp en millisecondes en chaîne de caractères lisible.

    Args:
        timestamp_ms (int): Timestamp en millisecondes.

    Returns:
        str: Date au format 'YYYY-MM-DD HH:MM:SS.mmm'
    """
    dt = datetime.utcfromtimestamp(timestamp_ms / 1000.0)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


# ------------------------ Capture des flux ----------------------- #
print(f"Lancement de l'analyse NFStreamer sur le fichier : {PCAP_INPUT_PATH}")
streamer = NFStreamer(
    source=PCAP_INPUT_PATH,
    idle_timeout=IDLE_TIMEOUT,
    active_timeout=ACTIVE_TIMEOUT,
    statistical_analysis=True,
    promiscuous_mode=True,
    bpf_filter="",
    n_dissections=10,
)
filtered_flows = list(streamer)  # Convertir en liste pour pouvoir trier
print("Flow :", filtered_flows[0])

# Trier par firstSeen (bidirectional_first_seen_ms)
filtered_flows = sorted(filtered_flows, key=lambda f: f.bidirectional_first_seen_ms)

# ------------------------- Export CSV --------------------------- #
print(f"Export des flux filtrés vers le fichier CSV : {CSV_OUTPUT_PATH}")

with open(CSV_OUTPUT_PATH, mode="w", newline="") as csv_file:
    writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)

    # Écriture de l'en-tête complet
    writer.writerow(
        [
            "firstSeen",
            "lastSeen",
            "srcIp",
            "dstIp",
            "srcPort",
            "dstPort",
            "protocol",
            "bytes",
            "packets",
            "durationMs",
            "totalBytesUpstream",
            "totalBytesDownstream",
            "totalPacketsUpstream",
            "totalPacketsDownstream",
            "minPacketLength",
            "maxPacketLength",
            "meanPacketLength",
            "stddevPacketLength",
            "bytesPerSecond",
            "packetsPerSecond",
            "interArrivalTimeMin",
            "interArrivalTimeMax",
            "interArrivalTimeMean",
            "interArrivalTimeStdDev",
            "synCount",
            "ackCount",
            "pshCount",
            "rstCount",
            "finCount",
        ]
    )

    # Tri des flux par firstSeen
    sorted_flows = sorted(filtered_flows, key=lambda f: f.bidirectional_first_seen_ms)

    for flow in sorted_flows:
        writer.writerow(
            [
                f"{timestamp_to_str(flow.bidirectional_first_seen_ms)}000000",
                f"{timestamp_to_str(flow.bidirectional_last_seen_ms)}000000",
                flow.src_ip,
                flow.dst_ip,
                flow.src_port,
                flow.dst_port,
                PROTOCOL_MAPPING.get(flow.protocol, "Unknown"),
                float(flow.bidirectional_bytes),
                float(flow.bidirectional_packets),
                float(flow.bidirectional_duration_ms),
                float(flow.src2dst_bytes),
                float(flow.dst2src_bytes),
                float(flow.src2dst_packets),
                float(flow.dst2src_packets),
                float(flow.bidirectional_min_ps),
                float(flow.bidirectional_max_ps),
                float(flow.bidirectional_mean_ps),
                float(flow.bidirectional_stddev_ps),
                float(
                    flow.bidirectional_bytes * 1000 / flow.bidirectional_duration_ms
                    if flow.bidirectional_duration_ms > 0
                    else 0
                ),
                float(
                    flow.bidirectional_packets * 1000 / flow.bidirectional_duration_ms
                    if flow.bidirectional_duration_ms > 0
                    else 0
                ),
                float(flow.bidirectional_min_piat_ms),
                float(flow.bidirectional_max_piat_ms),
                float(flow.bidirectional_mean_piat_ms),
                float(flow.bidirectional_stddev_piat_ms),
                flow.bidirectional_syn_packets,
                flow.bidirectional_ack_packets,
                flow.bidirectional_psh_packets,
                flow.bidirectional_rst_packets,
                flow.bidirectional_fin_packets,
            ]
        )


print("Export CSV terminé avec succès dans le fichier :", CSV_OUTPUT_PATH)
