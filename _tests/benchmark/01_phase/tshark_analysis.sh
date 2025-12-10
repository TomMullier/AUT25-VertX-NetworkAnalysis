#!/bin/bash
set -e

# Vérification des arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <PCAP_FILE> <CSV_FILE>"
    exit 1
fi

PCAP_FILE="$1"
CSV_FILE="$2"

# Extraction avec tshark
tshark -r "$PCAP_FILE" -T fields \
-o tcp.desegment_tcp_streams:FALSE \
-e frame.number \
-e ip.src \
-e ip.dst \
-e tcp.srcport \
-e tcp.dstport \
-e frame.len \
-E separator=, \
-E occurrence=f \
> "$CSV_FILE"

echo "Tshark output written to $CSV_FILE"
echo "Done."
