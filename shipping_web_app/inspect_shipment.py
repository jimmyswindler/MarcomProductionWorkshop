
from shared_lib.database import get_db_connection, get_real_dict_cursor
import sys

def inspect(ship_uid):
    conn = get_db_connection()
    if not conn:
        print("DB Connection failed")
        return

    cur = get_real_dict_cursor(conn)
    
    print(f"--- INSPECTING SHIPMENT {ship_uid} ---")
    
    # 1. Check Shipment Table
    cur.execute("SELECT * FROM shipments WHERE shipment_uid = %s", (ship_uid,))
    ship = cur.fetchone()
    print(f"SHIPMENT: {ship}")
    
    # 2. Check Linked Boxes
    cur.execute("SELECT * FROM item_boxes WHERE shipment_uid = %s", (ship_uid,))
    boxes = cur.fetchall()
    print(f"LINKED BOXES ({len(boxes)}):")
    for b in boxes:
        print(f" - {b['barcode_value']} (Item: {b['order_item_id']})")

    # 3. Check Floating Boxes (packed but no shipment?)
    # Just grab last 10 packed boxes
    cur.execute("SELECT * FROM item_boxes WHERE status='packed' ORDER BY packed_at DESC LIMIT 10")
    recent = cur.fetchall()
    print(f"RECENTLY PACKED BOXES (Last 10):")
    for b in recent:
        print(f" - {b['barcode_value']} | ShipUID: {b['shipment_uid']} | Packed: {b['packed_at']}")
        
    conn.close()

if __name__ == "__main__":
    inspect('20260212_0011')
