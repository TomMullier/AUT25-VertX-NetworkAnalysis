#!/bin/bash
set -e

INTERFACE="ens160"
PCAP_FILE="pcap/reference.pcap"
CSV_FILE="csv/tshark_output.csv"
PACKET_COUNT=10000

echo "Preparing directories..."

mkdir -p pcap
mkdir -p csv

chmod 755 pcap
chmod 755 csv

# Remove old pcap if exists
rm -f "$PCAP_FILE"

echo "Starting reference capture on interface $INTERFACE with tcpdump..."
echo "Saving $PACKET_COUNT packets to $PCAP_FILE"

tcpdump -i "$INTERFACE" -w "$PCAP_FILE" -c "$PACKET_COUNT"

echo "Capture completed. Processing pcap file..."

tshark -r "$PCAP_FILE" -T fields \
  -e frame.number \
  -e ip.src \
  -e ip.dst \
  -e tcp.srcport \
  -e tcp.dstport \
  -e frame.len \
  -E separator=, \
  > "$CSV_FILE"

echo "Tshark output written to $CSV_FILE"
echo "Done."
