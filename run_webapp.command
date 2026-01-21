#!/bin/bash
cd "$(dirname "$0")"
echo "Starting Shipping Web App..."
cd shipping_web_app
# Check if venv exists and activate it if so
if [ -d "venv" ]; then
    source venv/bin/activate
fi
python3 app.py
echo "Web App exited. Press any key to close."
read -n 1 -s
