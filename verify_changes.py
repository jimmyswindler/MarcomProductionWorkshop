
import os
import sys
import datetime
import glob
from shared_lib.database import get_db_connection

# Add root to path so we can import app modules if needed, 
# though we will mostly test DB logic directly or use the services
sys.path.append(os.getcwd())
try:
    from shipping_web_app.app.services.shipment_service import generate_worldship_xml
except ImportError:
    pass

def verify_id_generation():
    print("--- Verifying ID Generation Logic ---")
    conn = get_db_connection()
    if not conn:
        print("FAIL: Could not connect to DB")
        return

    cur = conn.cursor()
    
    # Simulate the logic we added
    now = datetime.datetime.now()
    date_str = now.strftime("%Y%m%d")
    
    # Check current DB state
    cur.execute("SELECT shipment_uid FROM shipments WHERE shipment_uid LIKE %s ORDER BY shipment_uid DESC LIMIT 5", (f"{date_str}_%",))
    rows = cur.fetchall()
    print(f"Current IDs for today ({date_str}): {[r[0] for r in rows]}")
    
    # We won't insert a record to avoid polluting production DB too much, 
    # but we can verify the SELECT logic finds the correct 'last_suffix'
    
    if rows:
        last_uid = rows[0][0]
        print(f"Last UID found: {last_uid}")
        try:
             last_suffix = int(last_uid.split('_')[-1])
             expected_next = last_suffix + 1
             print(f"Logic should generate suffix: {expected_next:04d}")
        except:
             print("Could not parse last UID suffix")
    else:
        print("No IDs found for today. Logic should generate suffix: 0001")
        
    conn.close()

def verify_simulation_globs():
    print("\n--- Verifying Simulation Globs ---")
    xml_output = 'xml_output'
    if not os.path.exists(xml_output):
        print("xml_output dir does not exist")
        return

    # Check matches for new pattern
    ptrn = os.path.join(xml_output, "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_*.xml")
    matches = glob.glob(ptrn)
    print(f"Files matching new XML pattern: {len(matches)}")
    if matches:
        print(f"Sample: {matches[0]}")
        
    out_ptrn = os.path.join(xml_output, "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_*.out")
    out_matches = glob.glob(out_ptrn)
    print(f"Files matching new OUT pattern: {len(out_matches)}")

if __name__ == "__main__":
    verify_id_generation()
    verify_simulation_globs()
