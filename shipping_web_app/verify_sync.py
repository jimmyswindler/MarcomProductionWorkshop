
from app.services import feedback_loop
from shared_lib.database import get_db_connection, get_real_dict_cursor
import os

# Force LIVE mode
os.environ['SIMULATION_MODE'] = 'False'

print("Running feedback loop cycle...")
try:
    c1, c2 = feedback_loop.run_feedback_cycle()
    print(f"Cycle complete. UPS Files Processed: {c1}, Marcom Responses: {c2}")

    # Check status of 20260212_0011
    conn = get_db_connection()
    cur = get_real_dict_cursor(conn)
    cur.execute("SELECT * FROM shipments WHERE shipment_uid = '20260212_0011'")
    row = cur.fetchone()
    if row:
        print(f"SHIPMENT 20260212_0011: Tracking={row.get('tracking_number')}, SyncStatus={row.get('marcom_sync_status')}, Msg={row.get('marcom_response_message')}")
    else:
        print("SHIPMENT 20260212_0011 NOT FOUND")
        
    conn.close()

except Exception as e:
    print(f"Error running cycle: {e}")
