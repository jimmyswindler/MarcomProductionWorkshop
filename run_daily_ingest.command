#!/bin/bash

# Wrapper script for Daily Ingest - Runs via LaunchAgent
# Explicitly set PATH to ensure standard binaries are found
export PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin

# 1. Navigate to Project Root
cd "$(dirname "$0")" || { echo "Failed to cd to script directory"; exit 1; }

# 2. Activate Venv
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
else
    echo "ERROR: venv not found at $(pwd)/venv" >> "LOGS/launchagent.stderr.log"
    exit 1
fi

# 3. Define Paths
# Remote Input (Source)
REMOTE_INPUT="/Volumes/FTP/MarcomWebOrder_CRpass1705/PULL/"

# Local Staging (Dest for Copy)
LOCAL_STAGING="$(pwd)/_REPORT_INPUT"

# Log File
LOG_FILE="LOGS/daily_ingest_wrapper.log"
ERROR_LOG="LOGS/launchagent.stderr.log"

# Create logs dir if not exists
mkdir -p LOGS

# Timestamp
echo "--- Daily Ingest Started: $(date) ---" >> "$LOG_FILE"
echo "--- Context: User=$(whoami), PWD=$(pwd) ---" >> "$LOG_FILE"

# 4. Check Mount
if [ ! -d "$REMOTE_INPUT" ]; then
    echo "ERROR: Remote Volume not mounted: $REMOTE_INPUT" >> "$LOG_FILE"
    echo "ERROR: Remote Volume not mounted: $REMOTE_INPUT" >> "$ERROR_LOG"
    exit 1
fi

# 5. Run Script
python3 ingest_daily.py \
    --input-dir "$REMOTE_INPUT" \
    --local-staging-dir "$LOCAL_STAGING" \
    >> "$LOG_FILE" 2>> "$ERROR_LOG"

EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    echo "ERROR: ingest_daily.py failed with exit code $EXIT_CODE" >> "$LOG_FILE"
    echo "ERROR: ingest_daily.py failed with exit code $EXIT_CODE (See $LOG_FILE)" >> "$ERROR_LOG"
else
    echo "--- Daily Ingest Finished with Exit Code: $EXIT_CODE ---" >> "$LOG_FILE"
fi

exit $EXIT_CODE
