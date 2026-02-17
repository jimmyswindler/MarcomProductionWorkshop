
import sys
import os
sys.path.append(os.getcwd())
from shared_lib.database import get_db_connection, get_real_dict_cursor

def verify():
    conn = get_db_connection()
    if not conn: return

    cur = get_real_dict_cursor(conn)
    
    # Target Order from previous step
    order_num = 'TXRH-1447583' 
    
    print(f"Checking items for Order {order_num}...")
    
    query = """
    SELECT i.job_ticket_display_id, i.product_name, s.shipment_uid
    FROM shipments s
    JOIN orders o ON s.order_number = o.order_number
    JOIN jobs j ON o.id = j.order_id
    JOIN items i ON j.id = i.job_id
    WHERE s.order_number = %s
    """
    
    cur.execute(query, (order_num,))
    rows = cur.fetchall()
    
    if not rows:
        print("No items found via Order Number link.")
    else:
        print(f"Found {len(rows)} items:")
        for r in rows:
            print(f"  - {r['job_ticket_display_id']} ({r['product_name']})")

    conn.close()

if __name__ == "__main__":
    verify()
