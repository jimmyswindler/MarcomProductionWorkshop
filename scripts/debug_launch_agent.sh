#!/bin/bash

# Debug Script for Marcom Daily Ingest LaunchAgent
# This script searches for the LaunchAgent plist and checks its status.

echo "--- Searching for LaunchAgents referencing 'MarcomProductionWorkshop' ---"

# Search in User Agents
USER_AGENTS=$(find ~/Library/LaunchAgents -name "*.plist" -print0 | xargs -0 grep -l "MarcomProductionWorkshop")

# Search in System Agents (less likely but possible)
SYSTEM_AGENTS=$(find /Library/LaunchAgents -name "*.plist" 2>/dev/null | xargs grep -l "MarcomProductionWorkshop" 2>/dev/null)

FOUND_AGENTS="$USER_AGENTS $SYSTEM_AGENTS"

if [ -z "$FOUND_AGENTS" ]; then
    echo "WARNING: No matching LaunchAgent .plist found containing 'MarcomProductionWorkshop'."
    echo "Checking for 'daily_ingest'..."
    USER_AGENTS=$(find ~/Library/LaunchAgents -name "*.plist" -print0 | xargs -0 grep -l "daily_ingest")
    FOUND_AGENTS="$USER_AGENTS"
fi

if [ -z "$FOUND_AGENTS" ]; then
    echo "ERROR: Could not find any LaunchAgent plist for this project."
    echo "Please ensure the LaunchAgent is installed in ~/Library/LaunchAgents"
    exit 1
fi

echo "Found the following Agent(s):"
echo "$FOUND_AGENTS"
echo ""

for AGENT_PATH in $FOUND_AGENTS; do
    echo "--- Inspecting $AGENT_PATH ---"
    # Extract Label
    LABEL=$(defaults read "$AGENT_PATH" Label 2>/dev/null)
    if [ -z "$LABEL" ]; then
        # Fallback extraction if defaults fails or binary plist
        LABEL=$(grep -A1 "Label" "$AGENT_PATH" | tail -n1 | sed 's/.*<string>\(.*\)<\/string>.*/\1/' | tr -d '[:space:]')
    fi
    
    echo "Label: $LABEL"
    
    # Check Status
    echo "Status (launchctl list):"
    launchctl list | grep "$LABEL"
    
    echo ""
    echo "--- Suggested Fix ---"
    echo "If the status above shows a non-zero exit code (e.g. 1, 78), or if logs show 'Operation not permitted':"
    echo "1. Go to System Settings -> Privacy & Security -> Full Disk Access"
    echo "2. Ensure 'bash', 'Terminal', or 'python3' (depending on how it's called) is checked."
    echo "3. Try reloading the agent:"
    echo "   launchctl unload \"$AGENT_PATH\""
    echo "   launchctl load \"$AGENT_PATH\""
    echo "   # Or force a kickstart:"
    echo "   launchctl kickstart -k gui/$(id -u)/$LABEL"
    echo ""
done
