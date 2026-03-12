"""
Database Cleanup Script
Cleanses old testing data targeting orders placed on or before 2026/02/25.
"""
import sys
import os

# Ensure we can import from shared_lib by adding project root to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.append(project_root)

from shared_lib.database import get_db_connection

def perform_cleanup():
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to the database. Exiting.")
        sys.exit(1)

    cur = conn.cursor()
    target_date = '2026-02-25 23:59:59'
    
    try:
        # Start transaction
        cur.execute("BEGIN;")
        
        # 1. Delete associated Shipments first (No CASCADE relationship)
        # Identify the target orders and delete their corresponding shipments
        cur.execute(f"""
            DELETE FROM shipments 
            WHERE order_number IN (
                SELECT order_number FROM orders WHERE order_date <= '{target_date}'
            );
        """)
        shipments_deleted = cur.rowcount
        
        # 2. Delete Orders (This automatically cascades to jobs, items, and item_boxes)
        cur.execute(f"DELETE FROM orders WHERE order_date <= '{target_date}';")
        orders_deleted = cur.rowcount
        
        # Commit the transaction
        conn.commit()
        
        print("-" * 40)
        print("DATA CLEANSING COMPLETE")
        print("-" * 40)
        print(f"Target Date: records on or before {target_date}")
        print(f"Shipments Deleted: {shipments_deleted}")
        print(f"Orders Deleted: {orders_deleted}")
        print("(Jobs, Items, and Item Boxes were automatically deleted via CASCADE)")
        print("-" * 40)

    except Exception as e:
        # Rollback on error
        conn.rollback()
        print(f"Error during cleanup: {e}")
        print("Transaction rolled back.")
        sys.exit(1)
        
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    perform_cleanup()
