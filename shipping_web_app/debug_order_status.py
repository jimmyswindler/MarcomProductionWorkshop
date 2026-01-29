import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

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

def debug_order(lookup_id):
    conn = get_db_connection()
    if not conn: return
    
    print(f"\n--- DEBUGGING: {lookup_id} ---")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    # Check if Order
    cur.execute("SELECT id, order_number FROM orders WHERE order_number = %s", (lookup_id,))
    order = cur.fetchone()
    
    job_ids = []
    if order:
        print(f"Found ORDER: ID={order['id']}")
        cur.execute("SELECT id, job_ticket_number FROM jobs WHERE order_id = %s", (order['id'],))
        jobs = cur.fetchall()
        print(f"  Linked Jobs ({len(jobs)}): {', '.join([j['job_ticket_number'] for j in jobs])}")
        job_ids = [j['id'] for j in jobs]
    else:
        # Check if Job
        cur.execute("SELECT id, order_id, job_ticket_number FROM jobs WHERE job_ticket_number = %s", (lookup_id,))
        job = cur.fetchone()
        if job:
            print(f"Found JOB: ticket={job['job_ticket_number']} (Linked to Order ID {job['order_id']})")
            job_ids = [job['id']]
        else:
            print("  NOT FOUND in Orders or Jobs")
            return

    if not job_ids:
        print("  No Jobs found.")
        return

    # Check Boxes
    cur.execute("""
        SELECT i.sku, b.barcode_value, b.status, b.packed_at
        FROM item_boxes b
        JOIN items i ON b.order_item_id = i.order_item_id
        WHERE i.job_id = ANY(%s)
        ORDER BY i.sku, b.box_sequence
    """, (job_ids,))
    
    rows = cur.fetchall()
    total = len(rows)
    packed = sum(1 for r in rows if r['status'] == 'packed')
    
    print(f"  BOX STATS: Total={total}, Packed={packed}")
    print("  DETAILS:")
    for r in rows:
        print(f"    [{r['status'].upper().ljust(8)}] {r['sku']} - {r['barcode_value']} (Packed: {r['packed_at']})")

    conn.close()

if __name__ == "__main__":
    debug_order("TXRH-1425067") # The problem order
    debug_order("TXRH-1424427") # The working order
