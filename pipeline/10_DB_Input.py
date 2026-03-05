import sys
import os
import pandas as pd
import argparse
import yaml

# Setup path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from shared_lib.database import get_db_connection, get_real_dict_cursor

def load_config():
    with open(os.path.join(project_root, 'config', 'config.yaml'), 'r') as f:
        return yaml.safe_load(f)

def fetch_ready_jobs(conn):
    cur = get_real_dict_cursor(conn)
    
    # Query: Join Jobs, Orders, Items to get a flat list per Item
    # We filter by production_status = 'READY'
    
    query = """
        SELECT 
            j.id as job_id_db,
            j.job_ticket_number,
            j.production_status,
            j.production_batch_id,
            j.project_description,
            j.general_description,
            j.paper_description,
            j.press_instructions,
            j.bindery_instructions,
            j.shipping_instructions as job_shipping_instructions,
            
            o.order_number,
            o.order_date,
            o.ship_date,
            o.ship_to_company,
            o.ship_to_name,
            o.address1, o.address2, o.address3,
            o.city, o.state, o.zip, o.country,
            o.cost_center,
            
            i.order_item_id,
            i.product_id,
            i.product_name,
            i.product_description,
            i.sku,
            i.sku_description,
            i.quantity_ordered,
            i.file_url,
            i.print_filename
            
        FROM jobs j
        JOIN orders o ON j.order_id = o.id
        JOIN items i ON i.job_id = j.id
        WHERE j.production_status = 'READY'
    """
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return rows

def fetch_boxes_for_item(conn, order_item_id):
    if not order_item_id: return {}
    cur = conn.cursor()
    cur.execute("SELECT box_sequence, barcode_value FROM item_boxes WHERE order_item_id = %s", (order_item_id,))
    rows = cur.fetchall()
    cur.close()
    
    boxes = {}
    for r in rows:
        seq = r[0] # 1 based
        val = r[1]
        # Map 1 -> A, 2 -> B etc
        if 1 <= seq <= 8:
            char = chr(65 + seq - 1)
            boxes[f'box_{char}'] = val
    return boxes

def main():
    parser = argparse.ArgumentParser(description="DB Input Adapter for Pipeline")
    parser.add_argument("--output", required=True, help="Path to output Excel file")
    args = parser.parse_args()
    
    conn = get_db_connection()
    if not conn:
        print("DB Connection Failed")
        sys.exit(1)
        
    print("Fetching READY jobs...")
    rows = fetch_ready_jobs(conn)
    
    if not rows:
        print("No READY jobs found.")
        # Create empty DF with expected columns to prevent pipeline crash?
        # Or just exit status 0 but empty file?
        # Pipeline controller checks if file exists usually.
        # We will create an empty DF.
        df = pd.DataFrame()
    else:
        # We need to enrich with boxes
        data = []
        for r in rows:
            row = dict(r)
            boxes = fetch_boxes_for_item(conn, row['order_item_id'])
            row.update(boxes)
            
            # Map columns to match what legacy pipeline expects
            # Legacy col names: '1-up_output_file_url'
            row['1-up_output_file_url'] = row['file_url']
            
            # Legacy expected 'shipping_instructions' usually from Order?
            # Or Job? We have 'job_shipping_instructions'.
            # We'll map generic names.
            
            data.append(row)
            
        df = pd.DataFrame(data)
        
        # Update Status to IN_PROCESS
        job_ids = list(set([r['job_id_db'] for r in rows]))
        if job_ids:
            cur = conn.cursor()
            cur.execute("""
                UPDATE jobs 
                SET production_status = 'IN_PROCESS' 
                WHERE id IN %s
            """, (tuple(job_ids),))
            conn.commit()
            print(f"Updated {len(job_ids)} jobs to IN_PROCESS.")

    # Save to Excel
    print(f"Saving to {args.output}")
    # Create directory if needed
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_excel(args.output, index=False)
    
    conn.close()

if __name__ == "__main__":
    main()
