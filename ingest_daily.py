#!/usr/bin/env python3
import sys
import os
import argparse
import logging
import yaml
import shutil
import pandas as pd
from datetime import datetime
import json
import re

# Setup import path to include project root
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from shared_lib.database import get_db_connection
from shared_lib.ingest_helpers import (
    parse_orders_xml, 
    parse_job_tickets_xml, 
    calculate_ship_date, 
    calculate_box_requirements
)
from shared_lib.config import get_env_var

# Setup Logging
log_dir = os.path.join(project_root, 'LOGS')
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, f'ingest_{datetime.now().strftime("%Y%m%d")}.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

def load_config():
    config_path = os.path.join(project_root, 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def get_db_string(val):
    if pd.isna(val) or val == "":
        return None
    return str(val).strip()

def process_ingestion(input_dir, processed_dir, config, dry_run=False):
    logging.info(f"Starting Ingestion from: {input_dir}")
    
    # 1. Setup Resources
    conn = get_db_connection()
    if not conn:
        logging.error("Failed to connect to Database. Exiting.")
        return
    cur = conn.cursor()

    # 2. Scan Files
    try:
        all_files = os.listdir(input_dir)
    except FileNotFoundError:
        logging.error(f"Input directory not found: {input_dir}")
        return

    file_pairs = {}
    pattern = re.compile(r'^(?P<prefix>.*?)_(?P<type>Orders|JobTickets)_(?P<daterange>\d{8}_\d{4}_to_\d{8}_\d{4})_.*\.xml$')
    
    for f in all_files:
        match = pattern.match(f)
        if match:
            key = f"{match.group('prefix')}_{match.group('daterange')}"
            if key not in file_pairs:
                file_pairs[key] = {}
            file_pairs[key][match.group('type')] = f

    order_files = []
    ticket_files = []
    
    for key, pair in file_pairs.items():
        if 'Orders' in pair and 'JobTickets' in pair:
            order_files.append(pair['Orders'])
            ticket_files.append(pair['JobTickets'])
        else:
            logging.info(f"Skipping incomplete pair for {key}: {list(pair.keys())} found.")

    logging.info(f"Found {len(order_files)} complete Order/Ticket file pairs to process.")

    # Pair them? Or process widely? 
    # Logic from 10_DataCollection implies processing all found.
    # We will process order files primarily, then map ticket info if available.
    
    # Actually, create a DF from all Orders first
    all_orders_df = pd.DataFrame()
    for f in order_files:
        path = os.path.join(input_dir, f)
        df = parse_orders_xml(path)
        if not df.empty:
            df['source_file'] = f
            all_orders_df = pd.concat([all_orders_df, df], ignore_index=True)
            
            all_orders_df = pd.concat([all_orders_df, df], ignore_index=True)

    # Parse Tickets to enrich (Description fields mostly)
    all_tickets_df = pd.DataFrame()
    for f in ticket_files:
        path = os.path.join(input_dir, f)
        df = parse_job_tickets_xml(path)
        if not df.empty:
            all_tickets_df = pd.concat([all_tickets_df, df], ignore_index=True)

    # Merge Tickets info onto Orders
    if not all_tickets_df.empty and 'job_ticket_number' in all_orders_df.columns:
        # Dedupe tickets
        all_tickets_df.drop_duplicates(subset=['job_ticket_number'], inplace=True)
        merged_df = pd.merge(all_orders_df, all_tickets_df, on='job_ticket_number', how='left')
    else:
        merged_df = all_orders_df

    # Calculate Fields
    logging.info("Calculating derived fields (Ship Date, Boxes)...")
    merged_df['ship_date'] = merged_df['order_date'].apply(lambda x: calculate_ship_date(x))
    try:
        merged_df = calculate_box_requirements(merged_df, config)
    except Exception as e:
        logging.error(f"Box Calculation Error: {e}")

    # --- 2.5 Calculate Display IDs (Suffix Logic) ---
    logging.info("Calculating Job Ticket Display IDs...")
    # Group by Job Ticket
    if 'job_ticket_number' in merged_df.columns:
        # Check if we have multiple items per ticket
        # Sort by ticket then order_item_id to ensure stable suffixing
        merged_df.sort_values(by=['job_ticket_number', 'order_item_id'], inplace=True)
        
        # Create a temporary counter
        merged_df['item_rank'] = merged_df.groupby('job_ticket_number').cumcount() + 1
        merged_df['job_item_count'] = merged_df.groupby('job_ticket_number')['job_ticket_number'].transform('count')
        
        def calc_display_id(row):
            jt = str(row['job_ticket_number'])
            if row['job_item_count'] > 1:
                return f"{jt}-{row['item_rank']:02d}"
            else:
                return jt

        merged_df['job_ticket_display_id'] = merged_df.apply(calc_display_id, axis=1)
    else:
        merged_df['job_ticket_display_id'] = None

    count_new = 0
    count_exists = 0
    
    allow_dupes = config.get('paths', {}).get('allow_duplicate_ingest', False)

    for idx, row in merged_df.iterrows():
        order_num = get_db_string(row.get('order_number'))
        job_ticket = get_db_string(row.get('job_ticket_number'))
        order_item_id = get_db_string(row.get('order_item_id'))
        
        if not order_num or not job_ticket:
            continue

        # --- A. Check Existence ---
        cur.execute("SELECT id FROM orders WHERE order_number = %s", (order_num,))
        res = cur.fetchone()
        order_id = res[0] if res else None
        
        if order_id and not allow_dupes:
            # Check if job exists
            cur.execute("SELECT id FROM jobs WHERE job_ticket_number = %s", (job_ticket,))
            if cur.fetchone():
                count_exists += 1
                continue # Skip existing job

        # --- B. Address Validation ---
        # With the new architecture, we ingest all addresses as PENDING
        # and allow process_address_validation.py to validate them asynchronously.
        
        is_validated = False
        val_status = "PENDING"
        val_details = {}
        
        # Address vars
        addr_args = {
            'address1': get_db_string(row.get('address1')),
            'address2': get_db_string(row.get('address2')),
            'address3': get_db_string(row.get('address3')),
            'city': get_db_string(row.get('city')),
            'state': get_db_string(row.get('state')),
            'zip': get_db_string(row.get('zip')),
            'country': get_db_string(row.get('country')) or 'US'
        }
        
        # We process store_number via cost_center, no regex guessing here
        store_number = get_db_string(row.get('cost_center'))

        if dry_run:
            logging.info(f"[DRY RUN] Would insert Order {order_num} Ticket {job_ticket}")
            continue

        # --- C. UPSERT Order ---
        try:
            cur.execute("""
                INSERT INTO orders (
                    order_number, order_date, ship_date, 
                    ship_to_company, ship_to_name, 
                    address1, address2, address3, city, state, zip, country,
                    address_validated, address_validation_status, address_validation_details,
                    store_number, cost_center, production_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'NEW')
                ON CONFLICT (order_number) DO UPDATE SET
                    order_date = EXCLUDED.order_date,
                    ship_date = EXCLUDED.ship_date,
                    address1 = EXCLUDED.address1,
                    address2 = EXCLUDED.address2,
                    address3 = EXCLUDED.address3,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    zip = EXCLUDED.zip,
                    country = EXCLUDED.country,
                    address_validated = EXCLUDED.address_validated,
                    address_validation_status = EXCLUDED.address_validation_status,
                    address_validation_details = EXCLUDED.address_validation_details
                RETURNING id;
            """, (
                order_num, row.get('order_date'), row.get('ship_date'),
                get_db_string(row.get('ship_to_company')), get_db_string(row.get('ship_to_name')),
                addr_args['address1'], addr_args['address2'], addr_args['address3'],
                addr_args['city'], addr_args['state'], addr_args['zip'], addr_args['country'],
                is_validated, val_status, json.dumps(val_details),
                get_db_string(row.get('cost_center')), get_db_string(row.get('cost_center')), # Store Num fallback to cost center logic
            ))
            order_id = cur.fetchone()[0]

            # --- D. UPSERT Job ---
            cur.execute("""
                INSERT INTO jobs (
                    job_ticket_number, order_id, 
                    project_description, general_description, paper_description,
                    press_instructions, bindery_instructions, shipping_instructions,
                    production_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'NEW')
                ON CONFLICT (job_ticket_number) DO NOTHING
                RETURNING id;
            """, (
                job_ticket, order_id,
                get_db_string(row.get('project_description')), get_db_string(row.get('general_description')),
                get_db_string(row.get('paper_description')), get_db_string(row.get('press_instructions')), 
                get_db_string(row.get('bindery_instructions')), get_db_string(row.get('shipping_instructions'))
            ))
            job_res = cur.fetchone()
            if job_res:
                job_id = job_res[0]
            else:
                cur.execute("SELECT id FROM jobs WHERE job_ticket_number = %s", (job_ticket,))
                job_id = cur.fetchone()[0]

            # --- E. UPSERT Item ---
            cur.execute("""
                INSERT INTO items (
                    order_item_id, job_id, product_id, product_name, product_description,
                    sku, sku_description, quantity_ordered, cost_center, file_url,
                    job_ticket_display_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_item_id) DO UPDATE SET
                    job_ticket_display_id = EXCLUDED.job_ticket_display_id
                RETURNING id;
            """, (
                order_item_id, job_id,
                get_db_string(row.get('product_id')), get_db_string(row.get('product_name')),
                get_db_string(row.get('product_description')), get_db_string(row.get('sku')),
                get_db_string(row.get('sku_description')), row.get('quantity_ordered'),
                get_db_string(row.get('cost_center')), get_db_string(row.get('file_url')),
                get_db_string(row.get('job_ticket_display_id'))
            ))
            # If item inserted ...
            
            # --- F. Insert Box Data ---
            for i in range(8):
                col = f'box_{chr(65+i)}'
                barcode = row.get(col)
                if barcode:
                    cur.execute("""
                        INSERT INTO item_boxes (order_item_id, box_sequence, barcode_value)
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (order_item_id, i+1, str(barcode)))

            count_new += 1
            if count_new % 10 == 0:
                conn.commit()
                logging.info(f"Progress: Processed {count_new} new or updated order lines...")

        except Exception as e:
            logging.error(f"Error processing row {job_ticket}: {e}")
            conn.rollback()

    # --- 3.5 Update Job Instructions (Backfill and Guaranteed Consistency) ---
    count_updated_tickets = 0
    if not all_tickets_df.empty:
        logging.info("Ensuring all job tickets have instructions mapped...")
        for idx, row in all_tickets_df.iterrows():
            job_ticket = get_db_string(row.get('job_ticket_number'))
            if not job_ticket: continue
            
            general_desc = get_db_string(row.get('general_description'))
            paper_desc = get_db_string(row.get('paper_description'))
            press_inst = get_db_string(row.get('press_instructions'))
            bindery_inst = get_db_string(row.get('bindery_instructions'))
            shipping_inst = get_db_string(row.get('job_ticket_shipping_instructions'))
            
            # Skip update if no data to map? Or update to sync what's available
            if not dry_run:
                try:
                    cur.execute("""
                        UPDATE jobs SET 
                            general_description = COALESCE(%s, general_description),
                            paper_description = COALESCE(%s, paper_description),
                            press_instructions = COALESCE(%s, press_instructions),
                            bindery_instructions = COALESCE(%s, bindery_instructions),
                            shipping_instructions = COALESCE(%s, shipping_instructions)
                        WHERE job_ticket_number = %s
                    """, (general_desc, paper_desc, press_inst, bindery_inst, shipping_inst, job_ticket))
                    if cur.rowcount > 0: count_updated_tickets += 1
                except Exception as e:
                    logging.error(f"Failed to update instructions for ticket {job_ticket}: {e}")
            else:
                logging.info(f"[DRY RUN] Would update instructions for ticket {job_ticket}")

    conn.commit()
    conn.close()
    
    logging.info(f"Ingestion Complete. Processed {count_new} new/updated jobs. Skipped {count_exists} existing. Updated instructions for {count_updated_tickets} tickets.")
    
    # 4. Move Files (if active)
    if not dry_run and (count_new > 0 or count_updated_tickets > 0):
        processed_dir_final = os.path.join(processed_dir, 'processed') # Per user request, no date folder? 
        # Plan says: "processed files will move to smb://.../processed/ without dated subfolders"
        os.makedirs(processed_dir_final, exist_ok=True)
        
        # Local Staging Logic (Copy first, then move original)
        local_staging_dir = config.get('local_staging_dir') 
        if local_staging_dir:
            os.makedirs(local_staging_dir, exist_ok=True)
            logging.info(f"Local Staging Active: Copying processed files to {local_staging_dir}")

        for f in order_files + ticket_files:
            src = os.path.join(input_dir, f)
            dst = os.path.join(processed_dir_final, f)
            try:
                if os.path.exists(src):
                    # 1. Local Copy (if configured)
                    if local_staging_dir:
                        local_dst = os.path.join(local_staging_dir, f)
                        shutil.copy2(src, local_dst)
                        # logging.info(f"Copied to local staging: {f}")

                    # 2. Move to Remote Processed
                    shutil.move(src, dst)
                    logging.info(f"Moved {f} to {dst}")
            except Exception as e:
                logging.error(f"Failed to move/copy file {f}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Daily Data Ingestion Script")
    parser.add_argument("--input-dir", help="Path to input directory (overrides config)")
    parser.add_argument("--processed-dir", help="Path to processed directory (overrides config)")
    parser.add_argument("--local-staging-dir", help="Optional: Path to local folder to COPY files to before moving originals.")
    parser.add_argument("--dry-run", action="store_true", help="Run without DB commit or file moves")
    
    args = parser.parse_args()
    config = load_config()

    if args.local_staging_dir:
        config['local_staging_dir'] = args.local_staging_dir
    
    # Logic to normalize path if "smb://" is passed?
    # For now, just rely on standard path lookup.
    
    input_dir = args.input_dir or os.getenv("INGEST_INPUT_DIR") or config.get('paths', {}).get('stage1_collect', {}).get('input_dir')
    processed_dir = args.processed_dir or input_dir # Default to same relative root if not specified? 
    # Usually processed is within input root or sibling.
    
    # The config has `input_dir` = './_REPORT_INPUT'. 
    # processed usually goes to `input_dir/processed`.
    
    if not os.path.exists(input_dir):
        # Check if it's an SMB path text
        if str(input_dir).startswith("smb://"):
            print(f"ERROR: Input path is an SMB URL: {input_dir}")
            print("Please mount the volume first and pass the local mount path.")
            print("Example: /Volumes/FTP/MarcomWebOrder_CRpass1705/PULL/")
            sys.exit(1)
            
    process_ingestion(input_dir, processed_dir, config, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
