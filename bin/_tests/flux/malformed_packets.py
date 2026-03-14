from scapy.all import sendp
from scapy.all import IP, ICMP, Raw, Ether
import binascii

pkts = []

# 1) ICMP avec type invalide (ex: 255) — pas forcément standard
pkt1 = IP(dst="192.168.61.128")/ICMP(type=255, code=0)/Raw(b"X")
pkts.append(pkt1)

# 2) ICMP avec checksum ICMP volontairement incorrect (on fixe la valeur)
pkt2 = IP(dst="192.168.61.128")/ICMP(type=8, code=0, chksum=0x1234)/Raw(b"payload")
# Attention : Scapy recalculera certains champs à l'envoi mais pour l'écriture pcap la valeur forcée est gardée si on convertit en bytes.
pkts.append(pkt2)

# 3) Paquet IP avec IHL (Internet Header Length) trop petit -> entête IP tronqué (IHL devrait être >=5)
# Scapy autorise de fixer ihl ; cela produit une entête IP invalide
pkt3 = IP(dst="192.168.61.128", ihl=2)/ICMP(type=0, code=0)/Raw(b"A")
pkts.append(pkt3)

# 4) Paquet dont le champ total length est plus petit que l'en-tête (incohérence)
# On construit d'abord un paquet complet puis on tronque les bytes pour simuler un en-tête incomplet
raw_pkt4 = bytes(IP(dst="192.168.61.128")/ICMP()/Raw(b"DATA"))
# tronquer volontairement (conserver seulement une partie des octets)
truncated = raw_pkt4[:10]  # coupe nette -> paquet tronqué
# encapsuler dans Ether pour permettre wrpcap d'écrire des octets bruts
pkt4 = Ether()/Raw(truncated)
pkts.append(pkt4)

# 5) Paquet IP avec longueur totale trop petite (on force len)
pkt5 = IP(dst="192.168.61.128", len=10)/ICMP()/Raw(b"")
pkts.append(pkt5)

# 6) Paquet ICMP avec champ Type et Code cohérents mais payload malformé (données non-UTF)
pkt6 = IP(dst="192.168.61.128")/ICMP(type=8, code=0)/Raw(b"\x00\xff\xfe\xfd")
pkts.append(pkt6)



for i, pkt in enumerate(pkts, start=1):
        sendp(pkt, iface="ens160", verbose=False)
        print(f"Sent malformed packet {i}")
print("All malformed packets sent.")
