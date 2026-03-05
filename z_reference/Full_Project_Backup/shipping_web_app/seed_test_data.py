import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import sys

# Define connection using standard setup (assuming localhost, port 5432, user postgres, db marcom_shipping for simplicity, or we can just use the app's env)

# Let's write a python script that imports the app's DB connection
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'app')))

try:
    from shared_lib.database import get_db_connection
except ImportError:
    print("Cannot import db connection")
    sys.exit(1)

def seed_test_data():
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to DB")
        return
        
    cur = conn.cursor()
    
    try:
        # We need to find an order with multiple items to test partial states.
        # Let's find one.
        cur.execute("""
            SELECT o.order_number, count(i.id) as item_count, o.id as order_id
            FROM orders o
            JOIN jobs j ON o.id = j.order_id
            JOIN items i ON j.id = i.job_id
            GROUP BY o.order_number, o.id
            HAVING count(i.id) > 2
            LIMIT 1
        """)
        
        order = cur.fetchone()
        if not order:
            print("Could not find suitable order")
            return
            
        order_num = order[0]
        order_id = order[2]
        print(f"Using Order: {order_num}")
        
        # Get items for this order
        cur.execute("""
            SELECT i.order_item_id
            FROM items i
            JOIN jobs j ON i.job_id = j.id
            WHERE j.order_id = %s
        """, (order_id,))
        
        items = cur.fetchall()
        
        # CLEAR existing box and shipment data for this order to have a clean state
        item_ids = tuple([i[0] for i in items])
        cur.execute("DELETE FROM item_boxes WHERE order_item_id IN %s", (item_ids,))
        cur.execute("DELETE FROM shipments WHERE order_number = %s", (order_num,))
        
        # Now let's create boxes for all items (Let's say they each get 1 box, barcode = item_id)
        for i, item in enumerate(items):
            item_id = item[0]
            barcode = f"BOX-{item_id}"
            
            # Scenario:
            # Item 0: Shipped
            # Item 1: Packed
            # Item 2: Open
            
            if i == 0:
                # Shipped Setup
                ship_uid = f"SHIP-{item_id}"
                cur.execute("INSERT INTO shipments (shipment_uid, order_number, tracking_number, marcom_response_message, marcom_sync_status) VALUES (%s, %s, %s, %s, %s)",
                    (ship_uid, order_num, f"1Z9999999{item_id}", f"Item {item_id}: Code: 3", "SUCCESS"))
                cur.execute("INSERT INTO item_boxes (barcode_value, order_item_id, shipment_uid, status, packed_at) VALUES (%s, %s, %s, %s, NOW())",
                    (barcode, item_id, ship_uid, 'packed'))
                    
            elif i == 1:
                # Packed Setup
                cur.execute("INSERT INTO item_boxes (barcode_value, order_item_id, status, packed_at) VALUES (%s, %s, %s, NOW())",
                    (barcode, item_id, 'packed'))
                    
            else:
                # Open Setup (No box)
                pass
                
        conn.commit()
        print(f"Data Seeded Successfully for order {order_num}")
            
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    seed_test_data()
