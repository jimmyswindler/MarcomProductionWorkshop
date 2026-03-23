import os
import time
import shutil
import requests
import pandas as pd
import concurrent.futures
import sys
import argparse
import re
import traceback

import utils_ui 

# Setup path for shared_lib
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from shared_lib.database import get_db_connection

def sanitize_filename(filename):
    filename = str(filename).replace('/', '-')
    return re.sub(r'[\\:*?"<>|]', '', filename).strip()

def download_pdf(url, filepath):
    try:
        if os.path.exists(filepath):
            return True # idempotency

        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, stream=True, timeout=30)
        response.raise_for_status()
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192): 
                f.write(chunk)
        return True
    except Exception as e:
        # utils_ui.print_error(f"Download failed for {url}: {e}") # Keep worker silent, let main loop handle reporting if needed
        return False

def download_worker(task):
    idx, url, path = task
    success = download_pdf(url, path)
    return idx, path, success

def process_sheet_downloads(df, files_path, sheet_name):
    utils_ui.print_section(f"Checking Assets for: {sheet_name}")
    
    # Identify tasks
    download_tasks = []
    
    # We iterate rows to find URLs
    # We use a set of unique URLs to avoid double-downloading if the same file is used multiple times (unlikely but possible)
    # However, keeping it per-row is safer for the logic of "row N needs file X"
    
    rows_with_index = list(df.reset_index().to_dict('records'))
    
    for row in rows_with_index:
        job_num = str(row.get("job_ticket_number", ""))
        if not job_num or pd.isna(job_num): 
            continue
            
        file_base = sanitize_filename(job_num)
        url = row.get("1-up_output_file_url", "")
        
        if url and isinstance(url, str):
            dest_path = os.path.join(files_path, f"{file_base}.pdf")
            
            if url.startswith('http'):
                if not os.path.exists(dest_path):
                    download_tasks.append((row['index'], url, dest_path))
            else:
                # Handle local paths, ensuring relative paths resolve correctly from project_root
                if not os.path.isabs(url):
                    url = os.path.join(project_root, url)
                
                if os.path.exists(url):
                    # Local copy if not already there
                    if not os.path.exists(dest_path):
                       try: 
                           shutil.copy2(url, dest_path)
                       except Exception: 
                           pass # Fail silently here?
    
    if not download_tasks:
        utils_ui.print_info("No new files to download.")
        # Even if no download, we might need to update DB if it was already local?
        # But we only track 'tasks'. 
        return

    # Just iterating is slow if large.
    # But download_tasks is subset.
    # Actually row is available in the loop above.
    pass

    # Better approach: Collect updates in the main loop

    utils_ui.print_info(f"Downloading {len(download_tasks)} files...")
    
    success_count = 0
    fail_count = 0
    
    with utils_ui.create_progress() as progress:
        task = progress.add_task("Downloading...", total=len(download_tasks))
        
        # High concurrency for I/O bound tasks
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            futures = [executor.submit(download_worker, t) for t in download_tasks]
            
            for future in concurrent.futures.as_completed(futures):
                idx, path, success = future.result()
                if success:
                    success_count += 1
                else:
                    fail_count += 1
                progress.update(task, advance=1)
                
    if fail_count > 0:
        utils_ui.print_warning(f"Downloaded {success_count} files. Failed: {fail_count}.")
    else:
        utils_ui.print_success(f"Successfully acquired {success_count} files.")

    # --- DB Update ---
    try:
        conn = get_db_connection()
        if conn:
            cur = conn.cursor()
            updates_count = 0
            for row in rows_with_index:
                job_num = str(row.get("job_ticket_number", ""))
                if not job_num or pd.isna(job_num): continue
                
                # Re-calculate filename logic (or store it earlier)
                file_base = sanitize_filename(job_num)
                filename = f"{file_base}.pdf"
                
                order_item_id_raw = row.get('order_item_id')
                if pd.notna(order_item_id_raw) and order_item_id_raw:
                    # Clean up order_item_id which might have '.0' due to pandas floats
                    order_item_id = str(order_item_id_raw).replace('.0', '')
                    
                    if not getattr(sys.modules['__main__'].args, 'dry_run', False):
                        # Update Item with explicit text cast for safety
                        cur.execute("UPDATE items SET print_filename = %s WHERE order_item_id::text = %s", (filename, order_item_id))
                        updates_count += 1
                    else:
                        updates_count += 1 # Count it for logging purposes in dry run

            if not getattr(sys.modules['__main__'].args, 'dry_run', False):
                conn.commit()
                utils_ui.print_success(f"Updated DB with filenames for {updates_count} items.")
            else:
                conn.rollback()
                utils_ui.print_info(f"[DRY RUN] Would have updated DB filenames for {updates_count} items.")
                
            cur.close()
            conn.close()
    except Exception as db_e:
        utils_ui.print_error(f"Failed to update DB filenames: {db_e}")

def main(input_excel_path, files_base_folder):
    utils_ui.setup_logging(None)
    utils_ui.print_banner("40a - Acquire Job Assets")
    start_time = time.time()

    try:
        os.makedirs(files_base_folder, exist_ok=True)

        xls = pd.ExcelFile(input_excel_path)
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name)
            
            if df.empty: 
                continue

            sanitized_sheet_name = sanitize_filename(sheet_name)
            sheet_files_path = os.path.join(files_base_folder, sanitized_sheet_name)
            os.makedirs(sheet_files_path, exist_ok=True)

            process_sheet_downloads(df, sheet_files_path, sheet_name)

    except Exception as e:
        utils_ui.print_error(f"Acquisition Failed: {e}")
        traceback.print_exc()
        sys.exit(1)
    
    utils_ui.print_success(f"Asset Acquisition Step Complete: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="40a - Acquire Job Assets")
    parser.add_argument("input_excel_path", help="Input Excel")
    parser.add_argument("files_base_folder", help="Files Output Base")
    parser.add_argument("--dry_run", action="store_true", help="Skip DB commits")
    args = parser.parse_args()

    main(args.input_excel_path, args.files_base_folder)
