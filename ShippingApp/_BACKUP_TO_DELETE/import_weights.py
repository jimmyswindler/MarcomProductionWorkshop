import csv
import psycopg2
import sys
import os

# DB CONFIG - same as app.py
DB_NAME = "marcom_production_suite"
DB_USER = "jimmyswindler"
DB_HOST = "localhost"

def import_data():
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST)
        cur = conn.cursor()
        
        # 1. Import Product Map (IDs -> Categories)
        if os.path.exists('product_map.csv'):
            print("Importing product_map.csv...")
            count = 0
            with open('product_map.csv', 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    pid = row['product_id'].strip()
                    cat = row['category_name'].strip()
                    cur.execute("""
                        INSERT INTO product_categories (product_id, category_name)
                        VALUES (%s, %s)
                        ON CONFLICT (product_id) 
                        DO UPDATE SET category_name = EXCLUDED.category_name
                    """, (pid, cat))
                    count += 1
            print(f"Updated {count} product mappings.")
        else:
             print("Skipping product_map.csv (not found)")

        # 2. Import Shipping Rules (Category/Qty -> Mixed Box Weights)
        if os.path.exists('shipping_rules.csv'):
            print("Importing shipping_rules.csv...")
            count = 0
            with open('shipping_rules.csv', 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cat = row['category_name'].strip()
                    qty = int(row['quantity'])
                    
                    # Parse handle empty strings safely
                    def get_float(k): return float(row[k]) if row.get(k) else None
                    def get_int(k): return int(row[k]) if row.get(k) else 0

                    w_wt = get_float('white_box_weight')
                    b_wt = get_float('blue_box_weight')
                    w_qty = get_int('white_box_qty')
                    b_qty = get_int('blue_box_qty')
                    
                    cur.execute("""
                        INSERT INTO product_shipping_rules 
                        (category_name, quantity, white_box_weight, blue_box_weight, white_box_qty, blue_box_qty)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (category_name, quantity) 
                        DO UPDATE SET 
                            white_box_weight = EXCLUDED.white_box_weight,
                            blue_box_weight = EXCLUDED.blue_box_weight,
                            white_box_qty = EXCLUDED.white_box_qty,
                            blue_box_qty = EXCLUDED.blue_box_qty
                    """, (cat, qty, w_wt, b_wt, w_qty, b_qty))
                    count += 1
            print(f"Updated {count} shipping rules.")
        else:
            print("Skipping shipping_rules.csv (not found)")
        
        conn.commit() 
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    import_data()
