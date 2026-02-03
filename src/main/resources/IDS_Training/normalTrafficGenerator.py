import random
import time
import requests
from ftplib import FTP
import os

def generate_http_traffic():
        urls = [
                "http://example.com",
                "http://example.org",
                "http://example.net"
        ]
        try:
                url = random.choice(urls)
                response = requests.get(url)
                print(f"HTTP GET {url} - Status Code: {response.status_code}")
        except Exception as e:
                print(f"HTTP Traffic Error: {e}")

def generate_ftp_traffic():
        ftp_servers = [
                {"host": "ftp.dlptest.com", "user": "dlpuser", "passwd": "rNrKYTX9g7z3RgJRmxWuGHbeu"}
        ]
        try:
                server = random.choice(ftp_servers)
                ftp = FTP(server["host"])
                ftp.login(user=server["user"], passwd=server["passwd"])
                print(f"FTP Connected to {server['host']}")
                ftp.quit()
        except Exception as e:
                print(f"FTP Traffic Error: {e}")

def generate_icmp_traffic():
        hosts = ["8.8.8.8", "1.1.1.1", "192.168.61.1"]
        try:
                host = random.choice(hosts)
                response = os.system(f"ping -c 1 {host}")
                if response == 0:
                        print(f"ICMP Ping to {host} - Success")
                else:
                        print(f"ICMP Ping to {host} - Failed")
        except Exception as e:
                print(f"ICMP Traffic Error: {e}")

def main():
        while True:
                traffic_type = random.choice(["http", "ftp", "icmp"])
                if traffic_type == "http":
                        generate_http_traffic()
                elif traffic_type == "ftp":
                        generate_ftp_traffic()
                elif traffic_type == "icmp":
                        generate_icmp_traffic()
                time.sleep(random.uniform(1, 5))  # Wait between 1 to 5 seconds

if __name__ == "__main__":
        main()
