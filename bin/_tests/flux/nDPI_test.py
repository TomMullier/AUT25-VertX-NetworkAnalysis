#!/usr/bin/env python3
from scapy.all import *

TARGET = "192.168.61.128"

#########################
# 1) DNS malformé
#########################
def send_malformed_dns():
    pkt = IP(dst=TARGET)/UDP(dport=53)/Raw(b"\x00\x01\x00")
    send(pkt, verbose=False)
    print("[+] DNS malformé envoyé")

#########################
# 2) DNS opcode invalid
#########################
def send_fake_dns_opcode():
    pkt = IP(dst=TARGET)/UDP(dport=53)/Raw(b"\x12\x34\xF0\x00AAAA")
    send(pkt, verbose=False)
    print("[+] Faux DNS Opcode envoyé")

#########################
# 3) DHCP vide
#########################
def send_invalid_dhcp():
    pkt = IP(dst=TARGET)/UDP(sport=68, dport=67)/Raw(b"")
    send(pkt, verbose=False)
    print("[+] DHCP vide envoyé")

#########################
# 4) TLS version impossible
#########################
def send_impossible_tls():
    fake_tls = b"\x16\x7F\x7F\x00\x2F" + b"A"*20
    pkt = IP(dst=TARGET)/TCP(dport=443, flags="PA")/fake_tls
    send(pkt, verbose=False)
    print("[+] TLS impossible envoyé")

#########################
# 5) QUIC malformé
#########################
def send_fake_quic():
    pkt = IP(dst=TARGET)/UDP(dport=443)/b"\xC3" + b"\x00"*20
    send(pkt, verbose=False)
    print("[+] Faux QUIC envoyé")

#########################
# 6) HTTP méthode invalide
#########################
def send_invalid_http_method():
    pkt = IP(dst=TARGET)/TCP(dport=80, flags="PA")/b"BREACK /test HTTP/1.1\r\nHost: x\r\n\r\n"
    send(pkt, verbose=False)
    print("[+] Fake HTTP envoyé")

#########################
# 7) TCP ZeroWindow
#########################
def send_zero_window_tcp():
    pkt = IP(dst=TARGET)/TCP(flags="A", window=0)
    send(pkt, verbose=False)
    print("[+] TCP ZeroWindow envoyé")

#########################
# 8) TCP overlapping segments
#########################
def send_overlapping_tcp():
    base = IP(dst=TARGET)/TCP(seq=1000, flags="PA")/b"AAAAAAA"
    overlap = IP(dst=TARGET)/TCP(seq=998, flags="PA")/b"BBBBBBBB"
    send(base, verbose=False)
    send(overlap, verbose=False)
    print("[+] Overlapping TCP envoyés")

#########################
# 9) IP options invalides
#########################
def send_invalid_ip_options():
    pkt = IP(dst=TARGET, options=[IPOption("\x44\x04\x00\x00")])/TCP()
    send(pkt, verbose=False)
    print("[+] IP options invalides envoyé")

#########################
# 10) Faux SSH
#########################
def send_fake_ssh():
    pkt = IP(dst=TARGET)/TCP(dport=22, flags="PA")/b"SSH-2.0-OpenSSH_Fake\r\nAAAA"
    send(pkt, verbose=False)
    print("[+] Fake SSH envoyé")


if __name__ == "__main__":
    send_malformed_dns()
    send_fake_dns_opcode()
    send_invalid_dhcp()
    send_impossible_tls()
    #send_fake_quic()
    send_invalid_http_method()
    send_zero_window_tcp()
    send_overlapping_tcp()
    #send_invalid_ip_options()
    send_fake_ssh()
    print("[+] Tous les paquets de test ont été envoyés.")