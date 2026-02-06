#!/bin/bash
# Run the Marcom Admin Web App
cd "$(dirname "$0")"
source venv/bin/activate
python3 admin_web_app/app.py
