#!/bin/bash
# Move to the directory where this script resides
cd "$(dirname "$0")"

# Specify the expected name of the Excel file
EXCEL_FILE="shipping_data.xlsx"

if [ ! -f "$EXCEL_FILE" ]; then
    echo "Error: Could not find '$EXCEL_FILE' in $(pwd)"
    echo "Please ensure your Excel file is named '$EXCEL_FILE' and is in the same folder."
    echo ""
    read -p "Press [Enter] to exit..."
    exit 1
fi

echo "Running update_shipping_from_excel.py with $EXCEL_FILE..."
./venv/bin/python3 update_shipping_from_excel.py --file "$EXCEL_FILE"

echo ""
read -p "Press [Enter] to exit..."
