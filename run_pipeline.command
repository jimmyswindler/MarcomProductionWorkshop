#!/bin/bash
cd "$(dirname "$0")"
echo "Starting Marcom Production Pipeline..."

# Check if venv exists and activate it
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
else
    echo "WARNING: Virtual environment not found. Running with system python."
    echo "Please run: python3 -m venv venv && venv/bin/pip install -r requirements.txt"
fi

# Run the controller
# Note: Using 'python' or 'python3' after activation will use the venv's python
python pipeline/00_Controller.py

echo "Pipeline finished. Press any key to exit."
read -n 1 -s
