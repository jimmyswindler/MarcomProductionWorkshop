import pandas as pd
import psycopg2
import os
import sys
import glob
import argparse
import datetime
import argparse
import datetime
import yaml
import utils_ui

# --- DB Configuration ---
DB_NAME = "marcom_production_suite"
DB_USER = "jimmyswindler"
DB_HOST = "localhost"
DB_PORT = "5432"

def load_config(config_path="config.yaml"):
    if not os.path.exists(config_path):
        return {}
    try:
        with open(config_path, 'r') as f: return yaml.safe_load(f)
    except:
        return {}


def create_tables_if_not_exist(conn):
    cur = conn.cursor()
    # Item Boxes schema - supports dynamic sequence
    cur.execute("""
        CREATE TABLE IF NOT EXISTS item_boxes (
            id SERIAL PRIMARY KEY,
            order_item_id TEXT REFERENCES items(order_item_id) ON DELETE CASCADE,
            box_sequence INT,
            barcode_value TEXT,
            UNIQUE(order_item_id, box_sequence)
        );
    """)
    conn.commit()
    cur.close()

def connect_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            host=DB_HOST,
            port=DB_PORT
        )
        create_tables_if_not_exist(conn)
        return conn
    except Exception as e:
        utils_ui.print_error(f"Database connection/schema init failed: {e}")
        return None

def clean_value(val):
    """Handle NaN/Nat and string cleanup"""
    if pd.isna(val) or val == 'nan':
        return None
    return str(val).strip()

def ingest_data(staging_dir):
    utils_ui.print_banner("15 - Data Ingest (XLSX -> DB)")
    
    # 1. Find the report file
    # Look for MarcomOrderDate*.xlsx or Consolidated_Report*.xlsx
    # We want the most recent one.
    search_patterns = [
        os.path.join(staging_dir, "MarcomOrderDate*.xlsx"),
        os.path.join(staging_dir, "Consolidated_Report*.xlsx")
    ]
    
    found_files = []
    for pattern in search_patterns:
        found_files.extend(glob.glob(pattern))
        
    if not found_files:
        utils_ui.print_error(f"No report file found in {staging_dir}")
        sys.exit(1)
        
    # Sort by modification time, newest first
    found_files.sort(key=os.path.getmtime, reverse=True)
    target_file = found_files[0]
    utils_ui.print_info(f"Ingesting file: {os.path.basename(target_file)}")
    
    try:
        df = pd.read_excel(target_file, engine='openpyxl')
    except Exception as e:
        utils_ui.print_error(f"Failed to read Excel file: {e}")
        sys.exit(1)
        
    conn = connect_db()
    if not conn:
        sys.exit(1)
        
    # Create cursor
    cur = conn.cursor()
    
    stats = {'orders_new': 0, 'jobs_new': 0, 'items_new': 0}
    
    stats = {'orders_new': 0, 'jobs_new': 0, 'items_new': 0}
    
    # --- SAFEGUARD: Duplicate Ingest Check ---
    # Check if a meaningful number of Job Tickets from this file ALREADY EXIST.
    if 'job_ticket_number' in df.columns:
        # Get unique tickets from file
        file_tickets = df['job_ticket_number'].dropna().astype(str).unique().tolist()
        if file_tickets:
            # Check DB for these
            # chunking to avoid huge massive WHERE IN clauses if tens of thousands
            # but usually a few thousand is fine for postgres. limiting to first 1000 for safety check is enough context.
            check_subset = file_tickets[:1000]
            
            cur.execute("SELECT job_ticket_number FROM jobs WHERE job_ticket_number = ANY(%s)", (check_subset,))
            existing_tickets = [row[0] for row in cur.fetchall()]
            
            if existing_tickets:
                # Collision detected!
                config = load_config("config.yaml")
                # Look in 'paths' -> 'allow_duplicate_ingest' OR root level? User asked for it below dynamic_build_root.
                # In config.yaml structure, dynamic_build_root is inside 'paths'.
                # So we check config['paths']['allow_duplicate_ingest']
                
                allow_dupes = False
                if 'paths' in config and 'allow_duplicate_ingest' in config['paths']:
                    allow_dupes = config['paths']['allow_duplicate_ingest']
                
                if not allow_dupes:
                    utils_ui.print_banner("CRITICAL ERROR: DUPLICATE DATA DETECTED", "Safeguard Triggered")
                    utils_ui.print_error(f"Found {len(existing_tickets)} Job Tickets from this file ALREADY in the Database.")
                    utils_ui.print_error("This suggests you are re-ingesting old data (Production Violation).")
                    utils_ui.print_error("Production HALTED.")
                    utils_ui.print_info("To bypass (Testing Mode), set 'allow_duplicate_ingest: true' in config.yaml.")
                    sys.exit(1)
                else:
                    utils_ui.print_warning(f"Duplicate Data Detected ({len(existing_tickets)} matches), but 'allow_duplicate_ingest' is TRUE.")
                    utils_ui.print_warning("Proceeding in TESTING MODE.")

    try:
        for idx, row in df.iterrows():
            # --- ORDER ---
            order_num = clean_value(row.get('order_number'))
            if not order_num: continue # Skip minimal rows
            
            # Map columns
            order_data = {
                'order_number': order_num,
                'order_date': row.get('order_date'), # timestamp?
                'ship_date': row.get('ship_date'),
                'ship_to_company': clean_value(row.get('ship_to_company')),
                'ship_to_name': clean_value(row.get('ship_to_name')),
                'address1': clean_value(row.get('address1')),
                'address2': clean_value(row.get('address2')),
                'address3': clean_value(row.get('address3')),
                'address4': clean_value(row.get('address4')),
                'city': clean_value(row.get('city')),
                'state': clean_value(row.get('state')),
                'zip': clean_value(row.get('zip')),
                'country': clean_value(row.get('country')),
            }
            
            # Insert Order (handle duplicates)
            # using ON CONFLICT DO UPDATE to ensure we have the ID and latest data
            insert_order_sql = """
                INSERT INTO orders (
                    order_number, order_date, ship_date, 
                    ship_to_company, ship_to_name, 
                    address1, address2, address3, address4, 
                    city, state, zip, country
                ) VALUES (
                    %(order_number)s, %(order_date)s, %(ship_date)s,
                    %(ship_to_company)s, %(ship_to_name)s,
                    %(address1)s, %(address2)s, %(address3)s, %(address4)s,
                    %(city)s, %(state)s, %(zip)s, %(country)s
                )
                ON CONFLICT (order_number) DO UPDATE SET
                    ship_date = EXCLUDED.ship_date,
                    ship_to_name = EXCLUDED.ship_to_name
                RETURNING id;
            """
            cur.execute(insert_order_sql, order_data)
            order_id = cur.fetchone()[0]
            if cur.statusmessage.startswith("INSERT"): stats['orders_new'] += 1

            # --- JOB ---
            jt_num = clean_value(row.get('job_ticket_number'))
            if not jt_num: continue
            
            job_data = {
                'job_ticket_number': jt_num,
                'order_id': order_id,
                'project_description': clean_value(row.get('job_ticket_project_description', '')), # handle loose mapping
                'general_description': clean_value(row.get('general_description')),
                'paper_description': clean_value(row.get('paper_description')),
                'press_instructions': clean_value(row.get('press_instructions')),
                'bindery_instructions': clean_value(row.get('bindery_instructions')),
                'shipping_instructions': clean_value(row.get('job_ticket_shipping_instructions')),
                'special_instructions': clean_value(row.get('special_instructions'))
            }

            insert_job_sql = """
                INSERT INTO jobs (
                    job_ticket_number, order_id, 
                    project_description, general_description,
                    paper_description, press_instructions,
                    bindery_instructions, shipping_instructions,
                    special_instructions
                ) VALUES (
                    %(job_ticket_number)s, %(order_id)s,
                    %(project_description)s, %(general_description)s,
                    %(paper_description)s, %(press_instructions)s,
                    %(bindery_instructions)s, %(shipping_instructions)s,
                    %(special_instructions)s
                )
                ON CONFLICT (job_ticket_number) DO NOTHING
                RETURNING id;
            """
            cur.execute(insert_job_sql, job_data)
            res = cur.fetchone()
            if res:
                job_id = res[0]
                stats['jobs_new'] += 1
            else:
                # Already exists, fetch ID
                cur.execute("SELECT id FROM jobs WHERE job_ticket_number = %s", (jt_num,))
                job_id = cur.fetchone()[0]
                
            # --- ITEM ---
            item_id = clean_value(row.get('order_item_id'))
            if not item_id: continue
            
            # Handle Quantity
            try: q = int(row.get('quantity_ordered', 0))
            except: q = 0
            
            item_data = {
                'order_item_id': item_id,
                'job_id': job_id,
                'product_id': clean_value(row.get('product_id')),
                'product_name': clean_value(row.get('product_name')),
                'product_description': clean_value(row.get('product_description')),
                'sku': clean_value(row.get('sku')),
                'sku_description': clean_value(row.get('sku_description')),
                'quantity_ordered': q,
                'cost_center': clean_value(row.get('cost_center')),
                'file_url': clean_value(row.get('1-up_output_file_url'))
            }
            
            insert_item_sql = """
                INSERT INTO items (
                    order_item_id, job_id,
                    product_id, product_name, product_description,
                    sku, sku_description, quantity_ordered,
                    cost_center, file_url
                ) VALUES (
                    %(order_item_id)s, %(job_id)s,
                    %(product_id)s, %(product_name)s, %(product_description)s,
                    %(sku)s, %(sku_description)s, %(quantity_ordered)s,
                    %(cost_center)s, %(file_url)s
                )
                ON CONFLICT (order_item_id) DO NOTHING
                RETURNING id;
            """
            cur.execute(insert_item_sql, item_data)
            if cur.rowcount > 0:
                stats['items_new'] += 1

            # --- ITEM BOXES ---
            # Box columns box_A through box_H
            for i in range(8):
                suffix = chr(65+i) # A, B, ...
                col_name = f'box_{suffix}'
                barcode_val = clean_value(row.get(col_name))
                
                if barcode_val:
                    box_data = {
                        'order_item_id': item_id,
                        'box_sequence': i + 1,
                        'barcode_value': barcode_val
                    }
                    insert_box_sql = """
                        INSERT INTO item_boxes (order_item_id, box_sequence, barcode_value)
                        VALUES (%(order_item_id)s, %(box_sequence)s, %(barcode_value)s)
                        ON CONFLICT (order_item_id, box_sequence) DO UPDATE SET
                            barcode_value = EXCLUDED.barcode_value;
                    """
                    cur.execute(insert_box_sql, box_data)


        conn.commit()
        utils_ui.print_success(f"Ingest Complete. New Records -> Orders: {stats['orders_new']}, Jobs: {stats['jobs_new']}, Items: {stats['items_new']}")

    except Exception as e:
        conn.rollback()
        utils_ui.print_error(f"Ingest Error: {e}")
        # print(traceback.format_exc()) # if we import traceback
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("staging_dir")
    args = parser.parse_args()
    ingest_data(args.staging_dir)
