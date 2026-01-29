import os
import time
import glob
import random
import datetime

# Output Folder (Same as app.py and process_ups_output.py)
XML_OUTPUT_FOLDER = os.path.join('shipping_web_app', 'xml_output')

print(f"Monitoring {XML_OUTPUT_FOLDER} for new XML files to simulate WorldShip response...")

while True:
    try:
        # Find all XML files
        xml_files = glob.glob(os.path.join(XML_OUTPUT_FOLDER, "SHIP_*.xml"))
        
        for xml_file in xml_files:
            # Check if .out exists
            base_name = os.path.splitext(os.path.basename(xml_file))[0]
            out_file = os.path.join(XML_OUTPUT_FOLDER, f"{base_name}.out")
            processed_xml = os.path.join(XML_OUTPUT_FOLDER, "processed", f"{base_name}.xml")
            
            # If out file doesn't exist AND it hasn't been processed yet
            if not os.path.exists(out_file) and not os.path.exists(processed_xml):
                print(f"Detected new shipment: {base_name}")
                print("Simulating processing delay...")
                time.sleep(2) 
                
                # Generate Mock Tracking
                tracking_num = f"1ZSIM{random.randint(1000000000, 9999999999)}"
                
                # Create .out content
                out_content = f"""<?xml version="1.0"?>
<OpenShipments>
    <OpenShipment>
        <ProcessMessage>
            <ShipID>{base_name}</ShipID>
            <TrackingNumbers>
                <TrackingNumber>{tracking_num}</TrackingNumber>
            </TrackingNumbers>
        </ProcessMessage>
    </OpenShipment>
</OpenShipments>"""
                
                with open(out_file, "w") as f:
                    f.write(out_content)
                    
                print(f"Generated {out_file} with Tracking {tracking_num}")
        
        time.sleep(2)
        
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5)
