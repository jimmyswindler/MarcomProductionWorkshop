#!/bin/bash

# Wrapper script to run the ingest daemon
cd "$(dirname "$0")" || { echo "Failed to cd to script directory"; exit 1; }

# Activate Venv if it exists
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
else
    echo "Warning: venv not found at $(pwd)/venv"
fi

# Configuration
REMOTE_INPUT="/Volumes/FTP/MarcomWebOrder_CRpass1705/PULL/"
INTERVAL_SECONDS=300

# Ensure logs directory exists
mkdir -p LOGS

echo "================================================="
echo " Starting Data Ingest Polling Daemon"
echo " Monitoring: $REMOTE_INPUT"
echo " Interval: Every $INTERVAL_SECONDS seconds."
echo " Press Ctrl+C to stop."
echo "================================================="

# Start the daemon
python3 ingest_daemon.py --input-dir "$REMOTE_INPUT" --interval "$INTERVAL_SECONDS"

exit 0
