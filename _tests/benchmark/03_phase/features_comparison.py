import csv
import ipaddress
import sys
import math

# ----------------------------------- Paths ---------------------------------- #
# csv_a_path = "./csv/nfstream_features_reference.csv"
# csv_b_path = "./csv/vertx_flows_features_reference.csv"
csv_a_path = "./csv/nfstream_features_benign_slowloris.csv"
csv_b_path = "./csv/vertx_flows_features_benign_slowloris.csv"
diff_output_path = "./csv/flows_differences.csv"

csv.field_size_limit(sys.maxsize)


# ------------------------------ Utils functions ----------------------------- #
def normalize_ip(ip):
    try:
        return str(ipaddress.ip_address(ip))
    except ValueError:
        return ip


threshold_percent = 0.0001


def float_eq_percent(a, b):
    """
    Compare deux floats et retourne True si leur différence relative
    est inférieure au seuil en pourcentage.

    threshold_percent: tolérance en pourcentage (ex: 1.0 = 1%)
    """
    # Si les deux sont exactement égaux
    if a == b:
        return True

    # Pour éviter division par zéro
    if a == 0 and b == 0:
        return True
    elif a == 0 or b == 0:
        # un des deux est zéro → différence infinie
        return False

    # Différence relative
    diff_percent = abs(a - b) / max(abs(a), abs(b)) * 100
    return diff_percent <= threshold_percent


# --------------------------------- Open CSV --------------------------------- #
def open_csv(path):
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)
    header = rows[0]
    data = rows[1:]
    return header, data


csv_a_header, csv_a_data = open_csv(csv_a_path)
csv_b_header, csv_b_data = open_csv(csv_b_path)

# ----------------------------- Column mappings ------------------------------ #
numeric_columns = [
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


# Pour simplifier, on crée un mapping automatique
def make_columns_map(header):
    return {col: header.index(col) for col in header}


csv_a_columns = make_columns_map(csv_a_header)
csv_b_columns = make_columns_map(csv_b_header)


# -------------------- Flow key constructor for comparison ------------------- #
def flowkey_constructor(flow, columns):
    src_ip = normalize_ip(flow[columns["srcIp"]])
    dst_ip = normalize_ip(flow[columns["dstIp"]])
    src_port = flow[columns["srcPort"]] or "0"
    dst_port = flow[columns["dstPort"]] or "0"
    protocol = flow[columns["protocol"]]
    if protocol == "IPv6-ICMP":
        protocol = "IPv6 Hop-by-Hop Option"
    if protocol == "ICMP":
        protocol = "ICMPv4"
    first_seen = flow[columns["firstSeen"]]
    return f"{src_ip}_{dst_ip}_{src_port}_{dst_port}_{protocol}_{first_seen}"


# ---------------------------- Build flow maps ------------------------------- #
csv_a_flows = {flowkey_constructor(flow, csv_a_columns): flow for flow in csv_a_data}
csv_b_flows = {flowkey_constructor(flow, csv_b_columns): flow for flow in csv_b_data}
# ---------------------------- Compare metrics ------------------------------ #
diff_count = 0
flow_count = 0
metric_diff_count = {col: 0 for col in numeric_columns}

with open(diff_output_path, "w", newline="") as f_out:
    writer = csv.writer(f_out)
    # Header CSV diff
    header = ["flowKey", "metric", "CSV_A", "CSV_B", "difference"]
    writer.writerow(header)

    for key, a_flow in csv_a_flows.items():
        b_flow = csv_b_flows.get(key)
        if not b_flow:
            continue  # Flow absent dans B, on ne compare pas ici

        flow_count += 1  # compte des flows comparés

        for col in numeric_columns:
            a_val = float(a_flow[csv_a_columns[col]])
            b_val = float(b_flow[csv_b_columns[col]])
            a_val = 0 if a_val != a_val else a_val
            b_val = 0 if b_val != b_val else b_val

            if not float_eq_percent(a_val, b_val):  # tolérance de 1%
                diff_count += 1
                metric_diff_count[col] += 1
                print(
                    f"Difference in flow {key}, metric {col}: A={a_val}; {math.trunc(a_val * 10000)} vs B={b_val}; {math.trunc(b_val * 10000)}"
                )
                writer.writerow(
                    [
                        key,
                        col,
                        f"{a_val:.4f}",
                        f"{b_val:.4f}",
                        f"{round(a_val - b_val, 4):.4f}",
                    ]
                )

print(f"Differences saved in {diff_output_path}")

# ---------------------------- Reporting summary ---------------------------- #
print("\n========= SUMMARY REPORT =========")

print("Metric comparison between :")
print(f"  CSV A: {csv_a_path}")
print(f"  CSV B: {csv_b_path}\n")

print("=================================")
print("Detection threshold for floats: " f"{threshold_percent}%\n")

print(f"Total flows compared: {flow_count}")
print(f"Total metric differences: {diff_count}")
print("\nDifferences per metric:")

for col in numeric_columns:
    if metric_diff_count[col] > 0:
        percent = metric_diff_count[col] / flow_count * 100
        print(f"  {col}: {metric_diff_count[col]} differences ({percent:.2f}%)")

print("=================================")
