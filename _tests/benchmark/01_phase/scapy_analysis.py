from scapy.all import rdpcap, IP, IPv6, TCP, UDP
import csv
import ipaddress

PCAP_FILE = "../000_PCAP/reference.pcap"
CSV_FILE = "./csv/scapy_output_reference.csv"

packets = rdpcap(PCAP_FILE)





with open(CSV_FILE, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    frame_number = 1
    for pkt in packets:

        ip_layer = None

        # IPv4
        if IP in pkt:
            ip_layer = pkt[IP]

        # IPv6
        elif IPv6 in pkt:
            ip_layer = pkt[IPv6]

        if ip_layer:
            src = ip_layer.src
            dst = ip_layer.dst

            # Détection de la couche L4 externe
            payload = ip_layer.payload

            sport = 0
            dport = 0

            if isinstance(payload, TCP):
                sport = payload.sport
                dport = payload.dport
            elif isinstance(payload, UDP):
                sport = payload.sport
                dport = payload.dport

            writer.writerow([frame_number, src, dst, sport, dport, len(pkt)])

        else:
            # Ni IPv4 ni IPv6 → on écrit 0 comme ton Java
            writer.writerow([frame_number, 0, 0, 0, 0, len(pkt)])

        frame_number += 1

print("CSV écrit dans", CSV_FILE)
