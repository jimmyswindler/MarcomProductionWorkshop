# 20a_DataSorter.py
import pandas as pd
import numpy as np
from tqdm import tqdm
import os
import re
import yaml
import sys
import traceback
import json
import argparse
import time

# --- CONFIGURATION ---
def load_config_from_path(config_path="config.yaml"):
    """Loads YAML configuration from a given path."""
    if not os.path.exists(config_path):
        print(f"FATAL ERROR: Configuration file not found at '{config_path}'")
        sys.exit(1)
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        print("✓ Configuration loaded successfully from path.")
        return config
    except yaml.YAMLError as e:
        print(f"FATAL ERROR: Could not parse YAML file: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"FATAL ERROR: An unexpected error occurred while loading the config: {e}")
        sys.exit(1)

def safe_get_list(config_dict, key_path):
    """Helper for safe list access in config."""
    keys = key_path.split('.')
    value = config_dict
    try:
        for key in keys:
            value = value.get(key)
            if value is None: return []
        return value if isinstance(value, list) else []
    except (AttributeError, TypeError):
        return []

# --- DATA ORGANIZATION (From original 20_DataBundling.py) ---
def organize_by_product_id(input_file, config):
    """
    MODIFIED: Handles blank/uncategorized IDs by moving them to an exceptions DataFrame.
    Returns a dictionary: {'categorized': categorized_dfs, 'exceptions': exceptions_df}
    """
    col_names = config.get('column_names', {})
    col_pid = col_names.get('product_id'); col_job = col_names.get('job_ticket_number'); col_qty = col_names.get('quantity_ordered')
    col_order = col_names.get('order_number'); col_paper = col_names.get('paper_description'); col_ord_date = col_names.get('order_date')
    col_prod_desc = col_names.get('product_description'); col_sku = col_names.get('sku'); col_ship_date = col_names.get('ship_date')
    col_base_job = 'Base Job Ticket Number'; col_job_total_lines = 'job_total_line_items'

    required_config_cols = ['product_id', 'job_ticket_number', 'quantity_ordered', 'order_number','paper_description', 'order_date', 'product_description', 'sku', 'ship_date']
    missing_keys_in_config = [col for col in required_config_cols if col not in col_names]
    
    if missing_keys_in_config:
        print(f"FATAL ERROR: Config missing required columns: {missing_keys_in_config}") # Use print
        return None

    print(f"\nLoading input file: {input_file}") # Use print
    dtype_map = { col_order: str, col_pid: str, col_job: str, col_sku: str }
    try:
        date_cols_to_parse = []
        if col_ord_date: date_cols_to_parse.append(col_ord_date)
        if col_ship_date: date_cols_to_parse.append(col_ship_date)

        df = pd.read_excel(input_file, dtype=dtype_map, parse_dates=date_cols_to_parse) if input_file.endswith('.xlsx') \
             else pd.read_csv(input_file, dtype=dtype_map, parse_dates=date_cols_to_parse)
        print(f"Successfully loaded {len(df)} records") # Use print
    except Exception as e:
        print(f"FATAL ERROR loading input file: {str(e)}") # Use print
        return None

    original_columns = df.columns.tolist()
    required_cols_in_file = [col_pid, col_job, col_qty, col_order, col_paper, col_ord_date, col_prod_desc, col_sku]
    actual_cols = set(df.columns); missing_cols_in_file = [col for col in required_cols_in_file if col not in actual_cols]
    if missing_cols_in_file:
        missing_keys_for_user = [k for k, v in col_names.items() if v in missing_cols_in_file]
        print(f"FATAL ERROR: Input file missing required columns: {missing_cols_in_file} (Config keys: {missing_keys_for_user})") # Use print
        return None

    # --- Data Cleaning & Preparation ---
    df[col_qty] = pd.to_numeric(df[col_qty], errors='coerce').fillna(0)
    original_row_count = len(df); df = df[df[col_qty] > 0].copy()
    rows_dropped = original_row_count - len(df)
    if rows_dropped > 0: print(f"INFO: Dropped {rows_dropped} rows with zero/invalid quantity.") # Use print
    if df.empty:
        print("WARNING: No rows with quantity > 0 after cleaning.") # Use print
        return {'categorized': {'_initial_stats': {}, '_original_columns': original_columns}, 'exceptions': pd.DataFrame(columns=original_columns)}

    df[col_pid] = df[col_pid].astype(str).str.strip().str.split('.').str[0]

    # --- Handle Blank Product IDs ---
    # MODIFICATION: This block no longer immediately moves blank IDs to exceptions.
    # We initialize the exceptions_df and let blanks flow into categorization.
    exceptions_df = pd.DataFrame(columns=df.columns)
    # --- END MODIFICATION ---

    df[col_base_job] = df[col_job].astype(str).str.replace(r'-\d{2}$', '', regex=True)
    if col_base_job in df.columns: df[col_job_total_lines] = df.groupby(col_base_job)[col_base_job].transform('count')
    else: df[col_job_total_lines] = 1
    
    # --- Initial Stats ---
    df_initial_orders = df[col_order].nunique() if col_order in df.columns else 0; df_initial_jobs = df[col_base_job].nunique() if col_base_job in df.columns else 0
    df_initial_rows = len(df); df_initial_qty = df[col_qty].sum() if col_qty in df.columns else 0
    print("\n" + "="*50); print("=       PRE-CATEGORIZATION FILE SUMMARY          ="); print("="*50) # Use print
    # MODIFICATION: Removed line that mentioned num_blank_pids
    print(f"- Total Unique Orders Found:    {df_initial_orders}"); print(f"- Total Unique Jobs Found:      {df_initial_jobs}") # Use print
    print(f"- Total Line Items (Rows):      {df_initial_rows}"); print(f"- Total Quantity Ordered:       {int(df_initial_qty):,}") # Use print

    # --- Job Ticket Renaming ---
    print("\nApplying universal job ticket renaming...") # Use print
    df[col_base_job] = df[col_base_job].astype(str); groups = df.groupby(col_base_job)
    for base_ticket, group in tqdm(groups, total=len(groups), desc="Renaming Jobs"):
        if len(group) > 1:
            group_sorted = group.sort_index()
            for i, idx in enumerate(group_sorted.index):
                 if idx in df.index: df.loc[idx, col_job] = f"{base_ticket}-{i + 1:02d}"
    print("Universal job ticket renaming complete.") # Use print

    # --- Phase 1: Dynamic Vectorized Categorization ---
    print("\nPhase 1: Performing vectorized initial categorization...") # Use print
    conditions, choices = [], []; product_id_categories = config.get('product_ids', {})
    bc_identifiers = safe_get_list(config, 'categorization_rules.business_card_identifiers')
    bc_ident_regex = '|'.join(re.escape(ident) for ident in bc_identifiers) if bc_identifiers else '(?!)'
    
    # --- MODIFICATION: Replaced faulty type check ---
    cond_bc_paper = pd.Series(False, index=df.index)
    if col_paper in df.columns:
        # NEW: Force the column to be a string before checking it.
        # This fixes the bug where NaN values caused the type check to fail.
        cond_bc_paper = df[col_paper].astype(str).str.contains(bc_ident_regex, case=False, na=False)
    # --- END MODIFICATION ---
    
    # --- MODIFICATION: This logic was corrected in a previous step to point to the correct category ---
    for category_name, id_list in product_id_categories.items():
        if not isinstance(id_list, list) or not id_list: continue
        cond_pid = df[col_pid].isin(id_list)
        # This logic is now: (PID matches) OR (Category is '16ptBusinessCard' AND Paper matches)
        conditions.append(cond_pid | cond_bc_paper if category_name == '16ptBusinessCard' else cond_pid); choices.append(category_name)
    # --- END MODIFICATION ---
    
    if conditions: df['Category'] = np.select(conditions, choices, default='Uncategorized')
    else: print("WARNING: No valid categories defined in config."); df['Category'] = 'Uncategorized' # Use print
    print("✓ Phase 1 complete.") # Use print

    # --- NEW: Phase 1.5: Blank ID Rescue ---
    print("\nPhase 1.5: Rescuing 'Uncategorized' items based on paper stock...")
    
    # Create the mask for 'Uncategorized' items
    uncategorized_mask = (df['Category'] == 'Uncategorized')
    
    # Re-use the paper stock check from Phase 1.
    # We must ensure cond_bc_paper is defined, which it is (line 151-156)
    
    # Combine the conditions: Must be 'Uncategorized' AND match the paper stock
    rescue_mask = uncategorized_mask & cond_bc_paper
    
    num_rescued = rescue_mask.sum()
    if num_rescued > 0:
        # Apply the rescue
        df.loc[rescue_mask, 'Category'] = '16ptBusinessCard' # This must match the config key
        print(f"  - Rescued {num_rescued} 'Uncategorized' rows to '16ptBusinessCard' based on paper stock.")
    else:
        print("  - No 'Uncategorized' rows matched the paper stock rescue rule.")
    print("✓ Phase 1.5 complete.")
    # --- END NEW BLOCK ---

    # --- Phase 2: Job-Aware Re-categorization Rules ---
    print("\nPhase 2: Applying re-categorization rules..."); print("- Applying Job-Aware Precedence Rules...") # Use print
    job_categories = df.groupby(col_base_job)['Category'].unique(); mixed_jobs = job_categories[job_categories.apply(len) > 1]
    
    # --- MODIFICATION: This rule was demoting "rescued" 16pt cards. It is now disabled. ---
    # jobs_to_uncat = mixed_jobs[mixed_jobs.apply(lambda x: 'Uncategorized' in x)].index
    # if len(jobs_to_uncat) > 0: print(f"  - Forcing {len(jobs_to_uncat)} mixed jobs (incl. Uncategorized) to 'Uncategorized'."); df.loc[df[col_base_job].isin(jobs_to_uncat), 'Category'] = 'Uncategorized' # Use print
    # --- END MODIFICATION ---

    job_categories = df.groupby(col_base_job)['Category'].unique(); mixed_jobs = job_categories[job_categories.apply(len) > 1] # Re-check
    jobs_to_pod = mixed_jobs[mixed_jobs.apply(lambda x: 'PrintOnDemand' in x and 'Uncategorized' not in x)].index
    if len(jobs_to_pod) > 0: print(f"  - Forcing {len(jobs_to_pod)} mixed jobs (incl. POD) to 'PrintOnDemand'."); df.loc[df[col_base_job].isin(jobs_to_pod), 'Category'] = 'PrintOnDemand' # Use print
    
    qty_threshold = config.get('categorization_rules', {}).get('high_quantity_threshold', float('inf'))
    if isinstance(qty_threshold, (int, float)) and col_qty in df.columns and col_base_job in df.columns:
        # --- MODIFICATION: This rule logic was 'BounceBack'/'BusinessCard', which was too vague.
        # It should match the actual category names from the config.
        high_qty_mask = (df['Category'].isin(['12ptBounceBack', '16ptBusinessCard'])) & (df[col_qty] > qty_threshold)
        # --- END MODIFICATION ---
        if high_qty_mask.any():
            jobs_to_move_qty = df.loc[high_qty_mask, col_base_job].unique()
            if len(jobs_to_move_qty) > 0: print(f"- Moving {len(jobs_to_move_qty)} jobs (> {qty_threshold}) to '25up layout'."); df.loc[df[col_base_job].isin(jobs_to_move_qty), 'Category'] = '25up layout' # Use print
    else: print("- Skipping high quantity re-categorization.") # Use print
    
    # --- URL Rule: Move items with blank 1-up URL to PrintOnDemand ---
    col_url = config.get('column_names', {}).get('one_up_output_file_url')
    
    if col_url and col_url in df.columns:
        # Check specifically for the 1-up URL column being blank/empty
        has_no_url_content = df[col_url].isnull() | (df[col_url].astype(str).str.strip() == '')
        
        # Only apply to specific categories
        eligible_url_mask = df['Category'].isin(['12ptBounceBack', '16ptBusinessCard']) & has_no_url_content
        
        if eligible_url_mask.sum() > 0:
            jobs_to_move_url = df.loc[eligible_url_mask, col_base_job].unique()
            if len(jobs_to_move_url) > 0: 
                print(f"- Moving {len(jobs_to_move_url)} jobs with empty '{col_url}' to 'PrintOnDemand'.")
                df.loc[df[col_base_job].isin(jobs_to_move_url), 'Category'] = 'PrintOnDemand'
    else: 
        print(f"- Column '{col_url}' not found or not configured. Skipping URL rule.")

    # --- Handle Uncategorized Items ---
    # This block now correctly catches all items that are 'Uncategorized'
    # *after* Phase 1 and Phase 1.5 have run.
    uncategorized_mask = (df['Category'] == 'Uncategorized')
    num_uncategorized = uncategorized_mask.sum()
    if num_uncategorized > 0:
        print(f"\nINFO: Found {num_uncategorized} rows that resolved to 'Uncategorized'. Moving to 'exceptions' sheet.") # Use print
        exceptions_df = pd.concat([exceptions_df, df[uncategorized_mask]], ignore_index=True)
        df = df[~uncategorized_mask].copy()
        if df.empty:
             print("INFO: All remaining rows were uncategorized.") # Use print

    # --- Final Split & Stats ---
    categorized_dfs = {cat: df[df['Category'] == cat].copy() for cat in df['Category'].unique() if cat != 'Uncategorized'}
    initial_stats = {}
    print("\nCategorization complete. Final Stats (excluding exceptions):") # Use print
    if not categorized_dfs:
        print("- No rows remaining after moving exceptions.") # Use print
    for cat, cat_df in categorized_dfs.items():
        stats = {'rows': len(cat_df), 'qty': cat_df[col_qty].sum() if col_qty in cat_df.columns else 0,
                 'orders': cat_df[col_order].nunique() if col_order in cat_df.columns else 0, 'jobs': cat_df[col_base_job].nunique() if col_base_job in cat_df.columns else 0}
        initial_stats[cat] = stats; print(f"- {cat}: {stats['rows']} rows, Qty: {int(stats['qty']):,}, Orders: {stats['orders']}, Jobs: {stats['jobs']}") # Use print

    categorized_dfs['_initial_stats'] = initial_stats
    categorized_dfs['_original_columns'] = original_columns

    return {'categorized': categorized_dfs, 'exceptions': exceptions_df}
# --- END DATA ORGANIZATION ---


# --- NEW Main Function ---
def main(input_excel_path, output_dir, config_path):
    """
    Main execution function for sorting a single file.
    Reads the _UNSORTED file, runs organization logic,
    and saves the output to a _CATEGORIZED file with sheets.
    """
    print("\n" + "="*50); print(" PRODUCT DATA SORTER (20a) ".center(50, "=")); print("="*50 + "\n")

    central_config = load_config_from_path(config_path)
    start_time = time.time()
    
    # --- Define output file path ---
    base_name_unsorted, file_ext = os.path.splitext(os.path.basename(input_excel_path))

    if base_name_unsorted.endswith("_UNSORTED"):
        base_name_categorized = base_name_unsorted.replace("_UNSORTED", "_CATEGORIZED")
    else:
        # Fallback if the input file isn't named as expected
        print(f"WARNING: Input file name '{base_name_unsorted}{file_ext}' did not end with '_UNSORTED'.")
        base_name_categorized = f"{base_name_unsorted}_CATEGORIZED"
    
    final_output_path = os.path.join(output_dir, f"{base_name_categorized}{file_ext}")
    print(f"Input file: {input_excel_path}")
    print(f"Output (checkpoint) file will be: {final_output_path}")

    try:
        print("\n" + "="*50); print("=           ORGANIZING DATA              ="); print("="*50)
        organized_data = organize_by_product_id(input_file=input_excel_path, config=central_config)

        if organized_data is None:
            raise Exception("Data organization failed due to critical error (check logs).")

        # --- NEW: Save all categories to sheets in the checkpoint file ---
        print(f"\nSaving all categorized sheets to: {final_output_path}")
        
        categorized_dfs = organized_data.get('categorized', {})
        exceptions_df = organized_data.get('exceptions', pd.DataFrame())
        
        # Pop metadata
        original_columns = categorized_dfs.pop('_original_columns', [])
        initial_stats = categorized_dfs.pop('_initial_stats', {})
        
        if not original_columns and not exceptions_df.empty:
            original_columns = exceptions_df.columns.tolist()
        
        # Add helper columns back in for the next step, if they exist
        helper_cols = ['Base Job Ticket Number', 'job_total_line_items', 'Category']
        for col in helper_cols:
            if col not in original_columns and any(col in df.columns for df in categorized_dfs.values()):
                original_columns.append(col)
            if col not in original_columns and col in exceptions_df.columns:
                original_columns.append(col)

        # Combine all sheets to be written
        sheets_to_write = {}
        sheets_to_write.update(categorized_dfs)
        
        if not exceptions_df.empty:
            sheets_to_write['exceptions'] = exceptions_df
            
        if not sheets_to_write:
            print("WARNING: No data to write (no categorized sheets or exceptions).")
            # Write an empty file anyway to signal completion
            pd.DataFrame().to_excel(final_output_path)
        else:
            with pd.ExcelWriter(final_output_path) as writer:
                for sheet_name, df_sheet in sheets_to_write.items():
                    if isinstance(df_sheet, pd.DataFrame):
                        print(f"  - Writing sheet: '{sheet_name}' ({len(df_sheet)} rows)")
                        # Ensure columns match the original file, plus any new helper cols
                        df_sheet.reindex(columns=original_columns).to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"\n✓ Categorization complete. Checkpoint file saved.")
        
    except Exception as e:
        print(f"\n⚠️ CRITICAL ERROR processing file {os.path.basename(input_excel_path)}: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        processing_time = time.time() - start_time
        print(f"\n--- Processing Time: {processing_time:.2f} seconds ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="20a - Sort and Categorize data into a checkpoint file.")
    parser.add_argument("input_excel_path", help="Path to the single input _UNSORTED Excel file.")
    parser.add_argument("output_dir", help="Directory to save the _CATEGORIZED Excel file.")
    parser.add_argument("config_path", help="Path to the central configuration YAML file.")

    args = parser.parse_args()

    main(args.input_excel_path, args.output_dir, args.config_path)