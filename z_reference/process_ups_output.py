import os
import time
import glob
import shutil
import xml.etree.ElementTree as ET
import psycopg2
import psycopg2.extras
import datetime
import json
from dotenv import load_dotenv

# Load Env
load_dotenv('.env')

# Configuration
XML_OUTPUT_FOLDER = os.path.join('shipping_web_app', 'xml_output')
PROCESSED_FOLDER = os.path.join(XML_OUTPUT_FOLDER, 'processed')

DB_NAME = os.getenv("DB_NAME", "marcom_production_suite")
DB_USER = os.getenv("DB_USER", "jimmyswindler")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

if not os.path.exists(PROCESSED_FOLDER):
    os.makedirs(PROCESSED_FOLDER)

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"DB Connection Error: {e}")
        return None

def parse_out_file(filepath):
    """
    Parses the UPS .out XML file and returns tracking numbers.
    Structure:
    <OpenShipments>
        <OpenShipment>
            <ProcessMessage>
                <TrackingNumbers>
                    <TrackingNumber>...</TrackingNumber>
    """
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
        
        # Namespace handling can be tricky in these files.
        # Often has xmlns="x-schema:OpenShipments.xdr"
        # We'll try to find tags regardless of namespace if possible, or strip it.
        
        tracking_numbers = []
        
        # Find all TrackingNumber tags recursively
        # This is a bit loose but robust for flat searches
        for elem in root.iter():
            if 'TrackingNumber' in elem.tag and elem.text:
                tn = elem.text.strip()
                if tn and tn not in tracking_numbers:
                    tracking_numbers.append(tn)
                    
        return tracking_numbers
    except Exception as e:
        print(f"Error parsing {filepath}: {e}")
        return []

def process_files():
    print(f"Checking {XML_OUTPUT_FOLDER} for .out files...")
    out_files = glob.glob(os.path.join(XML_OUTPUT_FOLDER, "*.out"))
    
    if not out_files:
        return

    conn = get_db_connection()
    if not conn:
        return

    for filepath in out_files:
        filename = os.path.basename(filepath)
        # Expected filename: SHIP_20260121_123045_1234.out
        # ShipmentUID is the basename without extension (usually, or we stripping extension)
        shipment_uid, _ = os.path.splitext(filename)
        
        print(f"Processing {shipment_uid}...")
        
        tracking_numbers = parse_out_file(filepath)
        
        if tracking_numbers:
            primary_tracking = tracking_numbers[0]
            
            try:
                cur = conn.cursor()
                
                # Check if shipment exists
                cur.execute("SELECT id FROM shipments WHERE shipment_uid = %s", (shipment_uid,))
                if cur.fetchone():
                    # Update
                    cur.execute("""
                        UPDATE shipments 
                        SET tracking_number = %s,
                            tracking_numbers = %s,
                            marcom_sync_status = 'PENDING',
                            marcom_response_message = 'Ready for sync',
                            updated_at = NOW()
                        WHERE shipment_uid = %s
                    """, (primary_tracking, json.dumps(tracking_numbers), shipment_uid))
                    
                    conn.commit()
                    print(f"Updated DB for {shipment_uid}: {tracking_numbers}")
                    
                    # Archive Files
                    # Move .out
                    shutil.move(filepath, os.path.join(PROCESSED_FOLDER, filename))
                    
                    # Move corresponding .xml if exists
                    xml_path = os.path.join(XML_OUTPUT_FOLDER, f"{shipment_uid}.xml")
                    if os.path.exists(xml_path):
                        shutil.move(xml_path, os.path.join(PROCESSED_FOLDER, f"{shipment_uid}.xml"))
                        
                else:
                    print(f"ShipmentUID {shipment_uid} not found in DB. Skipping archive to retry later (or manual check).")
                
                cur.close()
                
            except Exception as e:
                print(f"DB Error processing {shipment_uid}: {e}")
                conn.rollback()
        else:
            print(f"No tracking numbers found in {filename}.")

    conn.close()

if __name__ == "__main__":
    print("Starting UPS Output Processor...")
    while True:
        try:
            process_files()
        except Exception as e:
            print(f"Main Loop Error: {e}")
        
        time.sleep(10)
