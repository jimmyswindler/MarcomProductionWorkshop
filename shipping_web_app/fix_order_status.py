
import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Load env from the same dir as the app logic to ensure consistency
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

DB_NAME = os.getenv("DB_NAME", "marcom_production_suite")
DB_USER = os.getenv("DB_USER", "jimmyswindler")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def get_db_connection():
    try:
        return psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST, port=DB_PORT)
    except Exception as e:
        print(f"DB Connection Error: {e}")
        return None

def fix_order(lookup_id):
    conn = get_db_connection()
    if not conn: return
    
    print(f"\n--- FIXING: {lookup_id} ---")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    # 1. Get Jobs/Items logic (simplified from debug script)
    cur.execute("SELECT id FROM orders WHERE order_number = %s", (lookup_id,))
    order = cur.fetchone()
    
    job_ids = []
    if order:
        cur.execute("SELECT id FROM jobs WHERE order_id = %s", (order['id'],))
        jobs = cur.fetchall()
        job_ids = [j['id'] for j in jobs]
    
    if not job_ids:
        print("No jobs found.")
        return

    # 2. Update Box Status to 'packed'
    # We are updating ALL boxes for this order because the user said it was "known to have been shipped".
    # This simulates what SHOULD have happened.
    
    print(f"Updating boxes for Job IDs: {job_ids}")
    
    cur.execute("""
        UPDATE item_boxes 
        SET status = 'packed', packed_at = NOW()
        FROM items
        WHERE item_boxes.order_item_id = items.order_item_id
        AND items.job_id = ANY(%s)
        AND item_boxes.status != 'packed'
        RETURNING item_boxes.barcode_value
    """, (job_ids,))
    
    updated_rows = cur.fetchall()
    conn.commit()
    
    print(f"Updated {len(updated_rows)} boxes to 'packed'.")
    for r in updated_rows:
        print(f"  - {r['barcode_value']}")
        
    conn.close()

if __name__ == "__main__":
    fix_order("TXRH-1433227")
