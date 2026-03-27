# Testbed Attack Simulator & Orchestrator

This Python-based orchestration tool is designed to stress-test network sniffing and traffic capturing tools. It allows a central controller to manage a fleet of Raspberry Pis (or other Linux nodes) to generate coordinated, multi-vector attack traffic against a target system.



## Features
* **Dual-Stack Support:** Seamlessly handles IPv4 and IPv6 targets.
* **Unique Credentials:** Supports individual `User/Pass` combinations for every node in your testbed via CSV.
* **Multi-Vector Simulations:** Covers DDoS (TCP, UDP, DNS, SMTP, FTP), Recon, RCE, XSS, and MITM signatures.
* **Fine-Grained Control:** Adjust parallelism, thread counts, and packet frequency (sleep intervals).

---

## Prerequisites

### 1. Control Machine (Your PC)
Install the SSH communication library:
```bash
pip install paramiko
2. On the Raspberry Pis (Traffic Generators)
Ensure the following tools are installed to allow the script to execute the simulations:

sudo apt update
sudo apt install iperf3 nmap curl iputils-arping dnsutils netcat-openbsd
```
### 2. CMD execution examples:
```bash
Example 1: python tester.py --which-attack [TYPE] --pies [FILE] --target [IP] [OPTIONS]
Example 2: python tester.py --which-attack XSS --pies pies.txt --target 2001:db8::50 --sleep-interval 2.0
Example 3: python testbed_attack_sim.py --which-attack RECON --pies pies.txt --target 192.168.1.50 --parallelism yes
Example 4: python testbed_attack_sim.py --which-attack RECON --pies pies.txt --target 192.168.1.50 --parallelism yes --threads 4 --sleep-interval 0.5
````