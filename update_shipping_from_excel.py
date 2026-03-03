
import pandas as pd
import sys
import os
import argparse
from datetime import datetime

# Add project root to path for shared_lib
project_root = '/Users/jimmyswindler/Desktop/MarcomProductionWorkshop'
sys.path.append(project_root)

from shared_lib.database import get_db_connection, init_db

def update_shipping_data(excel_path, dry_run=False):
    print(f"--- Starting Shipping Update ---")
    print(f"Source File: {excel_path}")
    print(f"Dry Run: {dry_run}")
    
    # 1. Connect to DB
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to DB. Exiting.")
        return

    # Ensure Schema is up to date (will run migrations if needed)
    try:
        init_db(conn)
        print("Database schema verified/updated.")
    except Exception as e:
        print(f"Schema update failed: {e}")
        return

    cur = conn.cursor()

    # 2. Read Excel
    try:
        df = pd.read_excel(excel_path)
    except Exception as e:
        print(f"Failed to read Excel file: {e}")
        return
    
    processed_count = 0
    updated_orders = set()
    updated_jobs = set()
    
    for index, row in df.iterrows():
        order_item_id = str(row['Order Item ID']).strip()
        job_ticket = str(row['Job Ticket Number']).strip()
        order_number = str(row['Order Number']).strip()
        tracking_number = str(row['Tracking Number']).strip()
        
        # Parse Ship Date
        raw_ship_date = row['Ship Date']
        ship_date = None
        if pd.notna(raw_ship_date):
            ship_date = raw_ship_date
        
        if not order_number or not tracking_number:
            print(f"Skipping row {index}: Missing Order Number or Tracking Number")
            continue

        if dry_run:
            if index < 5:
                 print(f"[DRY RUN] Row {index}: Order {order_number}, Job {job_ticket}, Track {tracking_number}, Date {ship_date}")
            processed_count += 1
            continue

        try:
            # --- Update Order ---
            cur.execute("""
                UPDATE orders 
                SET production_status = 'SHIPPED', 
                    actual_ship_date = %s
                WHERE order_number = %s
                RETURNING id
            """, (ship_date, order_number))
            order_res = cur.fetchone()
            
            if order_res:
                order_id = order_res[0]
                updated_orders.add(order_id)
                
                # --- Update Job ---
                cur.execute("""
                    UPDATE jobs
                    SET production_status = 'SHIPPED'
                    WHERE job_ticket_number = %s
                    RETURNING id
                """, (job_ticket,))
                job_res = cur.fetchone()
                if job_res:
                     updated_jobs.add(job_res[0])
                
                # --- Insert/Update Shipment ---
                # Adapted to Actual DB Schema: 
                # shipment_uid (instead of shipment_id), order_number (instead of order_id)
                
                shipment_uid = tracking_number
                carrier = 'UPS'
                
                # Using order_number directly as per schema check
                cur.execute("""
                    INSERT INTO shipments (shipment_uid, order_number, tracking_number, carrier, status, ship_date)
                    VALUES (%s, %s, %s, %s, 'SHIPPED', %s)
                    ON CONFLICT (shipment_uid) DO UPDATE SET
                        status = 'SHIPPED',
                        ship_date = EXCLUDED.ship_date
                """, (shipment_uid, order_number, tracking_number, carrier, ship_date))
                
            else:
                print(f"Warning: Order {order_number} not found in DB.")
                
            processed_count += 1
            
            if processed_count % 50 == 0:
                conn.commit()
                print(f"Processed {processed_count} rows...")

        except Exception as e:
            print(f"Error processing row {index} ({order_number}): {e}")
            conn.rollback()

    if not dry_run:
        conn.commit()
        print(f"--- Update Complete ---")
        print(f"Processed Rows: {processed_count}")
        print(f"Unique Orders Updated: {len(updated_orders)}")
        print(f"Unique Jobs Updated: {len(updated_jobs)}")
    else:
        print(f"--- Dry Run Complete ---")
        print(f"Rows Scanned: {processed_count}")

    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update DB with Shipping Info from Excel")
    parser.add_argument("--file", required=True, help="Path to Excel File")
    parser.add_argument("--dry-run", action="store_true", help="Run without committing changes")
    
    args = parser.parse_args()
    update_shipping_data(args.file, args.dry_run)
