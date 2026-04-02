import os
import uuid
import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime

# File Paths & Config
EXCEL_PATH = '/Users/jimmyswindler/Desktop/MarcomProductionWorkshop/scripts/z_Open_Orders_0f6b2323-fa19-4188-b7ed-ce07363c1e71.xlsx'
DB_CONNECTION = 'dbname=marcom_production_suite user=jimmyswindler host=localhost port=5432'
BATCH_SIZE = 100

def main(dry_run=True):
    print(f"--- Starting Historical Import ({'DRY RUN' if dry_run else 'LIVE RUN'}) ---")
    
    # 1. Load Data
    try:
        df = pd.read_excel(EXCEL_PATH)
        total_initial = len(df)
        df = df.dropna(subset=['Tracking Number', 'Order Item ID'])
        print(f"Loaded {len(df)} rows with tracking info out of {total_initial} total.")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    # Create mapping from Order Item ID to Tracking Data
    # Convert 'Order Item ID' to string to match DB TEXT
    df['Order Item ID'] = df['Order Item ID'].astype(str)
    
    # 2. Extract unique tracking numbers & dates per Order Item ID
    item_data_map = {}
    for _, row in df.iterrows():
        item_id = str(row['Order Item ID'])
        # Extract date safely
        if pd.notna(row['Ship Date']):
            s_date = pd.to_datetime(row['Ship Date']).strftime('%Y-%m-%d %H:%M:%S')
        else:
            s_date = None
            
        item_data_map[item_id] = {
            'tracking_number': str(row['Tracking Number']).strip(),
            'ship_date': s_date
        }

    # 3. Connect to Database & Find Target Orders
    try:
        conn = psycopg2.connect(DB_CONNECTION)
    except Exception as e:
        print(f"DB Connection Error: {e}")
        return

    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # We query the DB for all these item IDs
        ids_in_excel = tuple(item_data_map.keys())
        if not ids_in_excel:
            print("No valid item IDs found.")
            return

        print("Querying database to match items...")
        cur.execute('''
            SELECT i.order_item_id, i.job_id, j.order_id, o.order_number, 
                   o.actual_ship_date, s.id as existing_shipment_id
            FROM items i
            JOIN jobs j ON j.id = i.job_id
            JOIN orders o ON o.id = j.order_id
            LEFT JOIN shipments s ON s.order_number = o.order_number
            WHERE i.order_item_id IN %s
        ''', (ids_in_excel,))
        
        db_rows = cur.fetchall()
        print(f"Found {len(db_rows)} matching items in DB.")

        # Filter out anything already shipped
        to_process = []
        already_shipped_count = 0
        for r in db_rows:
            if r['actual_ship_date'] is not None or r['existing_shipment_id'] is not None:
                already_shipped_count += 1
            else:
                to_process.append(r)
        
        print(f"Skipping {already_shipped_count} items that already have shipping data.")
        print(f"Remaining items to import: {len(to_process)}")

        if not to_process:
            print("Done. No new items to process.")
            return

        # 4. Aggregate data to Orders / Jobs / Shipments
        orders_to_update = {}  # {order_id: actual_ship_date}
        jobs_to_update = set() # set of job_ids
        shipments_to_insert = set() # set of (order_number, tracking_number, ship_date, order_id)
        
        for r in to_process:
            o_id = r['order_id']
            o_num = r['order_number']
            j_id = r['job_id']
            i_id = r['order_item_id']
            
            # Excel data for this item
            item_excel = item_data_map[i_id]
            s_date = item_excel['ship_date']
            t_num = item_excel['tracking_number']
            
            jobs_to_update.add(j_id)
            
            # Find max ship date for the order
            if o_id not in orders_to_update:
                orders_to_update[o_id] = s_date
            else:
                if s_date and orders_to_update[o_id]:
                    if s_date > orders_to_update[o_id]:
                        orders_to_update[o_id] = s_date
            
            # Create unique grouping for shipments
            # A single order could have shipped in multiple boxes (multiple tracking numbers)
            shipment_key = (o_num, t_num, s_date, o_id)
            shipments_to_insert.add(shipment_key)

        print(f"Aggregated -> Orders to Update: {len(orders_to_update)}")
        print(f"Aggregated -> Jobs to Update: {len(jobs_to_update)}")
        print(f"Aggregated -> Shipments to Create: {len(shipments_to_insert)}")

        if dry_run:
            print("\nDRY RUN mode. Exiting without modifying database.")
            return

        # 5. Execute DB Changes in Chunks
        print("\nBeginning Database Chunks...")
        
        # 5a. Shipments
        insert_shipments_sql = '''
            INSERT INTO shipments (shipment_uid, order_number, order_id, tracking_number, ship_date, marcom_sync_status, status, created_at)
            VALUES %s
        '''
        shipment_values = []
        for o_num, t_num, s_date, o_id in shipments_to_insert:
            uid = f"LEG_IMP_{uuid.uuid4().hex[:12]}"
            shipment_values.append((uid, o_num, o_id, t_num, s_date, 'EXTERNAL_IMPORT', 'SHIPPED', datetime.now()))

        psycopg2.extras.execute_values(cur, insert_shipments_sql, shipment_values, page_size=BATCH_SIZE)
        print(f"Inserted {len(shipment_values)} shipments.")

        # 5b. Orders Update (actual_ship_date, production_status)
        update_orders_sql = '''
            UPDATE orders SET actual_ship_date = data.s_date::timestamp, production_status = 'SHIPPED'
            FROM (VALUES %s) AS data(o_id, s_date)
            WHERE orders.id = data.o_id::int
        '''
        order_values = [(o_id, s_date) for o_id, s_date in orders_to_update.items()]
        psycopg2.extras.execute_values(cur, update_orders_sql, order_values, page_size=BATCH_SIZE)
        print(f"Updated {len(order_values)} orders.")

        # 5c. Jobs Update (production_status)
        update_jobs_sql = '''
            UPDATE jobs SET production_status = 'SHIPPED'
            WHERE id IN %s
        '''
        job_ids = list(jobs_to_update)
        for i in range(0, len(job_ids), BATCH_SIZE):
            chunk = tuple(job_ids[i:i + BATCH_SIZE])
            # If chunk has size 1, it becomes (id,) which translates to "IN (id)"
            cur.execute(update_jobs_sql, (chunk,))
        print(f"Updated {len(job_ids)} jobs.")

        # Commit everything safely
        conn.commit()
        print("\nAll changes committed successfully!")

    except Exception as e:
        conn.rollback()
        print(f"Transaction failed, rolling back changes: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    import sys
    dry = True
    if len(sys.argv) > 1 and sys.argv[1] == '--live':
        dry = False
    main(dry_run=dry)
