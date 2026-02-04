
import os
import glob
import random
import time
from datetime import datetime

# Configuration matching the app's structure
XML_DIR = 'xml_output'

def ensure_dirs():
    if not os.path.exists(XML_DIR):
        os.makedirs(XML_DIR)

def generate_tracking_number():
    # Simulate a UPS Ground tracking number: 1Z...
    return f"1Z{random.randint(100000, 999999)}A{random.randint(10000, 99999)}{random.randint(10, 99)}"

def simulate_ups_worldship_processing():
    """
    Scans for SHIP_*.xml files.
    If a corresponding .out file doesn't exist, create one after a short delay.
    """
    ensure_dirs()
    
    # Find all shipment XMLs
    # Match pattern YYYYMMDD_XXXX.xml
    xml_files = glob.glob(os.path.join(XML_DIR, "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_*.xml"))
    
    processed_count = 0
    
    for xml_file in xml_files:
        base_name = os.path.splitext(os.path.basename(xml_file))[0]
        out_file = os.path.join(XML_DIR, f"{base_name}.out")
        
        # If output doesn't exist, simulate UPS creating it
        if not os.path.exists(out_file):
            # Simulate processing time (randomly skip some to make it feel "live" if run rapidly, 
            # but for this demo trigger, just do it)
            
            tracking_num = generate_tracking_number()
            timestamp = datetime.now().strftime("%Y%m%d %H%M%S")
            
            # Simple content format for .out file:
            # SHIP_ID, TRACKING_NUMBER, COST, WEIGHT
            content = f"{base_name},{tracking_num},15.50,5.0,0"
            
            with open(out_file, 'w') as f:
                f.write(content)
            
            processed_count += 1
            
    return processed_count

def simulate_marcom_response():
    """
    Scans for SHIP_*.out files (meaning UPS processed it).
    Then checks if a MARCOM_CONFIRM_*.xml exists. If not, generate it.
    """
    ensure_dirs()
    
    out_files = glob.glob(os.path.join(XML_DIR, "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_*.out"))
    
    processed_count = 0
    
    for out_file in out_files:
        base_name = os.path.splitext(os.path.basename(out_file))[0]
        # base_name is SHIP_2023...
        
        confirm_file = os.path.join(XML_DIR, f"MARCOM_CONFIRM_{base_name}.xml")
        
        if not os.path.exists(confirm_file):
            # Create a mock Marcom response
            timestamp = datetime.now().isoformat()
            
            xml_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<MarcomResponse>
    <OriginalReference>{base_name}</OriginalReference>
    <Status>SUCCESS</Status>
    <Message>Order successfully received and processed.</Message>
    <Timestamp>{timestamp}</Timestamp>
</MarcomResponse>"""
            
            with open(confirm_file, 'w') as f:
                f.write(xml_content)
                
            processed_count += 1
            
    return processed_count
