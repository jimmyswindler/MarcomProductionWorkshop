
import sys
import os

sys.path.append(os.getcwd())

from shared_lib.database import get_db_connection, get_real_dict_cursor

def query_weights():
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to database")
        return

    try:
        cur = get_real_dict_cursor(conn)

        print("\n--- Product Shipping Rules (Item Weights) ---")
        cur.execute("SELECT category_name, quantity, box_weight FROM product_shipping_rules ORDER BY category_name, quantity")
        rules = cur.fetchall()
        print(f"{'Category':<30} | {'Qty':<5} | {'Weight (lbs)':<10}")
        print("-" * 50)
        for r in rules:
            cat = str(r['category_name']) if r['category_name'] is not None else "None"
            qty = str(r['quantity']) if r['quantity'] is not None else "None"
            w = str(r['box_weight']) if r['box_weight'] is not None else "None"
            print(f"{cat:<30} | {qty:<5} | {w:<10}")

        print("\n--- Shipping Cartons (Box Weights) ---")
        cur.execute("SELECT code, weight, length, width, height FROM shipping_cartons ORDER BY code")
        cartons = cur.fetchall()
        print(f"{'Code':<15} | {'Weight':<6} | {'L':<5} | {'W':<5} | {'H':<5}")
        print("-" * 50)
        for c in cartons:
            code = str(c['code']) if c['code'] is not None else "None"
            weight = str(c['weight']) if c['weight'] is not None else "None"
            l = str(c['length']) if c['length'] is not None else "None"
            w = str(c['width']) if c['width'] is not None else "None"
            h = str(c['height']) if c['height'] is not None else "None"
            print(f"{code:<15} | {weight:<6} | {l:<5} | {w:<5} | {h:<5}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    query_weights()
