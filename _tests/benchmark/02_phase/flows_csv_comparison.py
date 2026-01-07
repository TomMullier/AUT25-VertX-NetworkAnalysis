import csv
import ipaddress

# ----------------------------------- Paths ---------------------------------- #
# nfstream_path = "./csv/nfstream_flows_reference.csv"
# vertx_path = "./csv/vertx_flows_reference.csv"
nfstream_path = "./csv/nfstream_flows_benign_slowloris.csv"
vertx_path = "./csv/vertx_flows_benign_slowloris.csv"


# ------------------------------ Utils functions ----------------------------- #
def normalize_ip(ip):
    try:
        # convertit en IPv4 ou IPv6 canonique
        return str(ipaddress.ip_address(ip))
    except ValueError:
        # si ce n'est pas une IP valide, renvoie telle quelle
        return ip


# --------------------------------- Open CSV --------------------------------- #
def open_csv(path):
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)
    header = rows[0]
    data = rows[1:]
    return header, data


nfstream_header, nfstream_data = open_csv(nfstream_path)
vertx_header, vertx_data = open_csv(vertx_path)
# Print headers
# print("NFStream Header:", nfstream_header)
# print("Vert.x Header:", vertx_header)

nfstream_columns = {
    "src_ip": nfstream_header.index("src_ip"),
    "dst_ip": nfstream_header.index("dst_ip"),
    "src_port": nfstream_header.index("src_port"),
    "dst_port": nfstream_header.index("dst_port"),
    "protocol": nfstream_header.index("protocol"),
}

vertx_columns = {
    "src_ip": vertx_header.index("srcIp"),
    "dst_ip": vertx_header.index("dstIp"),
    "src_port": vertx_header.index("srcPort"),
    "dst_port": vertx_header.index("dstPort"),
    "protocol": vertx_header.index("protocol"),
}


# -------------------- Flow key constructor for comparison ------------------- #
def flowkey_constructor(flow, type):
    src_ip = ""
    dst_ip = ""
    src_port = ""
    dst_port = ""
    protocol = ""
    if type == "nfstream":
        src_ip = flow[nfstream_columns["src_ip"]]
        dst_ip = flow[nfstream_columns["dst_ip"]]
        src_port = flow[nfstream_columns["src_port"]]
        dst_port = flow[nfstream_columns["dst_port"]]
        protocol = flow[nfstream_columns["protocol"]]
    elif type == "vertx":
        src_ip = flow[vertx_columns["src_ip"]]
        dst_ip = flow[vertx_columns["dst_ip"]]
        src_port = flow[vertx_columns["src_port"]]
        dst_port = flow[vertx_columns["dst_port"]]
        protocol = flow[vertx_columns["protocol"]]
    if src_port == "":
        src_port = "0"
    if dst_port == "":
        dst_port = "0"
    if protocol == "ICMP":
        protocol = "ICMPv4"
    if protocol == "IPv6 Hop-by-Hop Option":
        protocol = "IPv6-ICMP"
    src_ip = normalize_ip(src_ip)
    dst_ip = normalize_ip(dst_ip)
    return src_ip + "_" + dst_ip + "_" + src_port + "_" + dst_port + "_" + protocol


nfstream_flows = {flowkey_constructor(flow, "nfstream"): flow for flow in nfstream_data}
vertx_flows = {flowkey_constructor(flow, "vertx"): flow for flow in vertx_data}

# ---------------------------- Comparison logic ------------------------------ #
# Copy for tracking remaining flows
vertx_remaining = vertx_flows.copy()
nf_remaining = {}

# Stats

nfstream_missing_count = 0
vertx_missing_count = 0
total_nfstream_flows = len(nfstream_flows)
total_vertx_flows = len(vertx_flows)


# check NFStream flows against Vert.x flows
for key, nf_flow in nfstream_flows.items():
    if key in vertx_remaining:
        del vertx_remaining[key]  # Remove found flows
    else:
        nf_remaining[key] = nf_flow  # This one is missing in Vert.x

# nf_remaining = NFStream flows not found in Vert.x
# vertx_remaining = Vert.x flows not found in NFStream

print("========== Flow Comparison Report ==========\n")

# Flows present in NFStream BUT missing in Vert.x
print("=== NFStream → Vert.x : Missing in Vert.x ===")
if len(nf_remaining) == 0:
    print("No flows are missing in Vert.x, NFStream is fully included.")
else:
    print(f"{len(nf_remaining)} flows are missing in Vert.x:")
    for k in list(nf_remaining.keys())[:20]:
        print("   -", k)
    if len(nf_remaining) > 20:
        print(f"   ... and {len(nf_remaining) - 20} others")

print("\n")

# Flows present in Vert.x BUT missing in NFStream
print("=== Vert.x → NFStream : Missing in NFStream ===")
if len(vertx_remaining) == 0:
    print("No flows are missing in NFStream, Vert.x is fully included.")
else:
    print(f"{len(vertx_remaining)} flows are missing in NFStream:")
    for k in list(vertx_remaining.keys())[:20]:
        print("   -", k)
    if len(vertx_remaining) > 20:
        print(f"   ... and {len(vertx_remaining) - 20} others")

print("\n========== Summary ==========")
print(f"Total NFStream flows: {total_nfstream_flows}")
print(f"Total Vert.x flows: {total_vertx_flows}")
print(f"Flows missing in Vert.x: {len(nf_remaining)}")
print(f"Flows missing in NFStream: {len(vertx_remaining)}")
# In %

if total_nfstream_flows > 0:
    print(
        f"Percentage of NFStream flows missing in Vert.x: {len(nf_remaining) / total_nfstream_flows * 100:.2f}%"
    )
    print(
        f"Success rate for NFStream: {100 - (len(nf_remaining) / total_nfstream_flows * 100):.2f}%"
    )
if total_vertx_flows > 0:
    print(
        f"Percentage of Vert.x flows missing in NFStream: {len(vertx_remaining) / total_vertx_flows * 100:.2f}%"
    )
    print(
        f"Success rate for Vert.x: {100 - (len(vertx_remaining) / total_vertx_flows * 100):.2f}%"
    )
print("=============================================")
