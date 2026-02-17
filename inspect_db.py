
import sys
import os
sys.path.append(os.getcwd())
from shared_lib.database import get_db_connection, get_real_dict_cursor

def inspect():
    conn = get_db_connection()
    if not conn:
        print("Failed to connect")
        return

    cur = get_real_dict_cursor(conn)
    
    print("\n--- Columns in 'items' table ---")
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'items'")
    for row in cur.fetchall():
        print(row['column_name'])

    print("\n--- Columns in 'shipments' table ---")
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'shipments'")
    for row in cur.fetchall():
        print(row['column_name'])

    print("\n--- Recent Shipments (Limit 5) ---")
    cur.execute("SELECT shipment_uid, tracking_number, created_at, order_number FROM shipments ORDER BY created_at DESC LIMIT 5")
    for row in cur.fetchall():
        print(f"UID: {row.get('shipment_uid')}, Tracking: {row.get('tracking_number')}, Order: {row.get('order_number')}")

    print("\n--- Join Test (Shipment -> Items) ---")
    # Try to replicate the query join to see what we match
    cur.execute("""
        SELECT s.shipment_uid, i.*
        FROM shipments s
        JOIN item_boxes ib ON s.shipment_uid = ib.shipment_uid
        JOIN items i ON ib.order_item_id = i.order_item_id
        LIMIT 2
    """)
    rows = cur.fetchall()
    if not rows:
        print("No items found linked to shipments.")
    else:
        for r in rows:
            print(r)

    conn.close()

if __name__ == "__main__":
    inspect()
