from scapy.all import rdpcap, IP, TCP, UDP
import csv

PCAP_FILE = "../000_PCAP/benign+slowloris_net_packets.pcap"
CSV_FILE = "./csv/scapy_output_benign_slowloris.csv"

packets = rdpcap(PCAP_FILE)

with open(CSV_FILE, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)

    frame_number = 1
    for pkt in packets:

        if IP not in pkt:
            print(f"Packet {frame_number} skipped: No IP layer found.")
            writer.writerow([frame_number, "", "", "", "", len(pkt)])
            frame_number += 1
            continue

        # IP EXTERNE UNIQUEMENT
        ip_outer = pkt[IP]
        src_ip = ip_outer.src
        dst_ip = ip_outer.dst

        sport = 0
        dport = 0

        # TCP EXTERNE (directement après l'IP externe uniquement)
        if isinstance(ip_outer.payload, TCP):
            sport = ip_outer.payload.sport
            dport = ip_outer.payload.dport

        # UDP EXTERNE (ex: VXLAN)
        elif isinstance(ip_outer.payload, UDP):
            sport = ip_outer.payload.sport
            dport = ip_outer.payload.dport

        writer.writerow([frame_number, src_ip, dst_ip, sport, dport, len(pkt)])
        frame_number += 1

print(f"CSV écrit dans {CSV_FILE}")
