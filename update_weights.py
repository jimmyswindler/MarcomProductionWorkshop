
import sys
import os

# Add current directory to path
sys.path.append(os.getcwd())

from shared_lib.database import get_db_connection

def update_weights():
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to database")
        return

    try:
        cur = conn.cursor()

        updates = [
            # 12ptBounceBack
            ("12ptBounceBack", 2500, 6.75),
            ("12ptBounceBack", 1000, 2.75),
            ("12ptBounceBack", 500, 1.35),
            ("12ptBounceBack", 250, 0.075),
            
            # 16ptBusinessCard
            ("16ptBusinessCard", 4000, 8.00),
            ("16ptBusinessCard", 1000, 4.00),
            ("16ptBusinessCard", 500, 2.00),
            ("16ptBusinessCard", 250, 1.00)
        ]

        print("Updating weights...")
        for category, qty, weight in updates:
            # Check if exists
            cur.execute("""
                SELECT 1 FROM product_shipping_rules 
                WHERE category_name = %s AND quantity = %s
            """, (category, qty))
            
            if cur.fetchone():
                # Update
                cur.execute("""
                    UPDATE product_shipping_rules 
                    SET box_weight = %s 
                    WHERE category_name = %s AND quantity = %s
                """, (weight, category, qty))
                print(f"Updated: {category} (Qty: {qty}) -> {weight} lbs")
            else:
                # Insert
                cur.execute("""
                    INSERT INTO product_shipping_rules (category_name, quantity, box_weight)
                    VALUES (%s, %s, %s)
                """, (category, qty, weight))
                print(f"Inserted: {category} (Qty: {qty}) -> {weight} lbs")

        conn.commit()
        print("Done.")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    update_weights()
