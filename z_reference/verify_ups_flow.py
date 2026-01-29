import os
import datetime
import subprocess
import psycopg2
import psycopg2.extras
import time
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("DB_NAME", "marcom_production_suite")
DB_USER = os.getenv("DB_USER", "jimmyswindler")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

XML_OUTPUT = "xml_output"

def get_db():
    return psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST, port=DB_PORT)

def test_flow():
    # 1. Setup Data
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    shipment_uid = f"TEST_SHIP_{timestamp}"
    test_out_file = os.path.join(XML_OUTPUT, f"{shipment_uid}.out")
    
    print(f"Preparing test for {shipment_uid}...")
    
    conn = get_db()
    cur = conn.cursor()
    
    # Clean previous (unlikely collision but good practice)
    cur.execute("DELETE FROM shipments WHERE shipment_uid = %s", (shipment_uid,))
    
    # Insert PENDING
    print("Inserting PENDING record...")
    cur.execute("INSERT INTO shipments (shipment_uid, marcom_sync_status) VALUES (%s, 'PROCESSING')", (shipment_uid,))
    conn.commit()
    
    # 2. Mock .out File
    xml_content = f"""<?xml version="1.0"?>
<OpenShipments>
    <OpenShipment>
        <ProcessMessage>
            <TrackingNumbers>
                <TrackingNumber>1ZTEST1234567890</TrackingNumber>
                <TrackingNumber>1ZTEST0987654321</TrackingNumber>
            </TrackingNumbers>
        </ProcessMessage>
    </OpenShipment>
</OpenShipments>"""
    
    with open(test_out_file, "w") as f:
        f.write(xml_content)
        
    print(f"Created mock file: {test_out_file}")
    
    # 3. Run Processor (using a one-off run via import is hard if loop is main, 
    # but we can import the function process_files from the script)
    # Be careful with venv if needed. Assuming current environment has dependencies.
    
    print("Running processor...")
    # Import locally to run the function
    from process_ups_output import process_files
    process_files()
    
    # 4. Verify DB
    print("Verifying DB update...")
    cur.execute("SELECT tracking_number, tracking_numbers, marcom_sync_status, updated_at FROM shipments WHERE shipment_uid = %s", (shipment_uid,))
    row = cur.fetchone()
    
    if row:
        print(f"Row State: {row}")
        
        # Check Tracking Number
        if row[0] == "1ZTEST1234567890":
            print("PASS: Primary Tracking Number set.")
        else:
            print(f"FAIL: Primary Tracking Number mismatch: {row[0]}")
            
        # Check JSON Tracking Numbers
        if "1ZTEST0987654321" in str(row[1]):
            print("PASS: JSON Tracking Numbers contains secondary.")
        else:
            print("FAIL: JSON Tracking Numbers incorrect.")
            
        # Check Status
        if row[2] == "PENDING": # Logic sets it to PENDING for sync
            print("PASS: Status set to PENDING.")
        else:
             print(f"FAIL: Status is {row[2]}")
             
        if row[3]:
            print("PASS: Updated At is set.")
        else:
             print("FAIL: Updated At is null.")
             
    else:
        print("FAIL: Record not found in DB.")
        
    cur.close()
    conn.close()
    
    # Cleanup
    processed_path = os.path.join(XML_OUTPUT, "processed", f"{shipment_uid}.out")
    if os.path.exists(processed_path):
        os.remove(processed_path)
        print("Cleaned up processed file.")
    if os.path.exists(test_out_file):
        os.remove(test_out_file)

if __name__ == "__main__":
    test_flow()
