# 20a_DataSorter.py
import pandas as pd
import numpy as np
import os
import re
import yaml
import sys
import traceback
import json
import argparse
import time
import datetime
import utils_ui  # <--- New UI Utility

# --- CONFIGURATION ---
def load_config_from_path(config_path="config.yaml"):
    """Loads YAML configuration from a given path."""
    if not os.path.exists(config_path):
        utils_ui.print_error(f"Configuration file not found at '{config_path}'")
        sys.exit(1)
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        # utils_ui.print_success("Configuration loaded successfully.")
        return config
    except yaml.YAMLError as e:
        utils_ui.print_error(f"Could not parse YAML file: {e}")
        sys.exit(1)
    except Exception as e:
        utils_ui.print_error(f"Unexpected error while loading config: {e}")
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

# --- DATA ORGANIZATION ---
def organize_by_product_id(input_file, config):
    col_names = config.get('column_names', {})
    col_pid = col_names.get('product_id'); col_job = col_names.get('job_ticket_number'); col_qty = col_names.get('quantity_ordered')
    col_order = col_names.get('order_number'); col_paper = col_names.get('paper_description'); col_ord_date = col_names.get('order_date')
    col_prod_desc = col_names.get('product_description'); col_sku = col_names.get('sku'); col_ship_date = col_names.get('ship_date')
    col_base_job = 'Base Job Ticket Number'; col_job_total_lines = 'job_total_line_items'

    required_config_cols = ['product_id', 'job_ticket_number', 'quantity_ordered', 'order_number','paper_description', 'order_date', 'product_description', 'sku', 'ship_date']
    missing_keys_in_config = [col for col in required_config_cols if col not in col_names]
    
    if missing_keys_in_config:
        utils_ui.print_error(f"Config missing required columns: {missing_keys_in_config}")
        return None

    utils_ui.print_info(f"Loading input file: {input_file}")
    dtype_map = { col_order: str, col_pid: str, col_job: str, col_sku: str }
    try:
        date_cols_to_parse = []
        if col_ord_date: date_cols_to_parse.append(col_ord_date)
        if col_ship_date: date_cols_to_parse.append(col_ship_date)

        df = pd.read_excel(input_file, dtype=dtype_map, parse_dates=date_cols_to_parse) if input_file.endswith('.xlsx') \
             else pd.read_csv(input_file, dtype=dtype_map, parse_dates=date_cols_to_parse)
        utils_ui.print_info(f"Loaded {len(df)} records")
    except Exception as e:
        utils_ui.print_error(f"Loading input file: {str(e)}")
        return None

    original_columns = df.columns.tolist()
    required_cols_in_file = [col_pid, col_job, col_qty, col_order, col_paper, col_ord_date, col_prod_desc, col_sku]
    actual_cols = set(df.columns); missing_cols_in_file = [col for col in required_cols_in_file if col not in actual_cols]
    if missing_cols_in_file:
        missing_keys_for_user = [k for k, v in col_names.items() if v in missing_cols_in_file]
        utils_ui.print_error(f"Input file missing required columns: {missing_cols_in_file}")
        return None

    # --- Data Cleaning ---
    df[col_qty] = pd.to_numeric(df[col_qty], errors='coerce').fillna(0)
    
    # Identify and separate zero/invalid quantity rows
    zero_qty_mask = df[col_qty] <= 0
    dropped_zero_qty_df = df[zero_qty_mask].copy()
    
    # Filter main dataframe
    df = df[~zero_qty_mask].copy()
    
    if not dropped_zero_qty_df.empty: 
        utils_ui.print_info(f"Dropped {len(dropped_zero_qty_df)} rows with zero/invalid quantity.")
        
    if df.empty:
        utils_ui.print_warning("No rows with quantity > 0 after cleaning.")
        return {'categorized': {'_initial_stats': {}, '_original_columns': original_columns}, 
                'exceptions': pd.DataFrame(columns=original_columns),
                'dropped_zero_qty': dropped_zero_qty_df}

    df[col_pid] = df[col_pid].astype(str).str.strip().str.split('.').str[0]
    exceptions_df = pd.DataFrame(columns=df.columns)

    df[col_base_job] = df[col_job].astype(str).str.replace(r'-\d{2}$', '', regex=True)
    if col_base_job in df.columns: df[col_job_total_lines] = df.groupby(col_base_job)[col_base_job].transform('count')
    else: df[col_job_total_lines] = 1
    
    # --- Initial Stats ---
    df_initial_orders = df[col_order].nunique() if col_order in df.columns else 0; df_initial_jobs = df[col_base_job].nunique() if col_base_job in df.columns else 0
    df_initial_rows = len(df); df_initial_qty = df[col_qty].sum() if col_qty in df.columns else 0
    
    utils_ui.print_section("Pre-Categorization Summary")
    utils_ui.print_info(f"Unique Orders: {df_initial_orders} | Unique Jobs: {df_initial_jobs}")
    utils_ui.print_info(f"Total Rows: {df_initial_rows} | Total Qty: {int(df_initial_qty):,}")

    # --- Job Ticket Renaming ---
    utils_ui.print_info("Applying universal job ticket renaming...")
    df[col_base_job] = df[col_base_job].astype(str); groups = df.groupby(col_base_job)
    
    # --- Using RICH Progress Bar ---
    with utils_ui.create_progress() as progress:
        task = progress.add_task("Renaming Jobs...", total=len(groups))
        for base_ticket, group in groups:
            if len(group) > 1:
                group_sorted = group.sort_index()
                for i, idx in enumerate(group_sorted.index):
                     if idx in df.index: df.loc[idx, col_job] = f"{base_ticket}-{i + 1:02d}"
            progress.update(task, advance=1)
            
    utils_ui.print_success("Renaming complete.")

    # --- Phase 1: Dynamic Vectorized Categorization ---
    utils_ui.print_info("Phase 1: Vectorized categorization...")
    conditions, choices = [], []; product_id_categories = config.get('product_ids', {})
    bc_identifiers = safe_get_list(config, 'categorization_rules.business_card_identifiers')
    bc_ident_regex = '|'.join(re.escape(ident) for ident in bc_identifiers) if bc_identifiers else '(?!)'
    
    cond_bc_paper = pd.Series(False, index=df.index)
    if col_paper in df.columns:
        cond_bc_paper = df[col_paper].astype(str).str.contains(bc_ident_regex, case=False, na=False)
    
    for category_name, id_list in product_id_categories.items():
        if not isinstance(id_list, list) or not id_list: continue
        cond_pid = df[col_pid].isin(id_list)
        conditions.append(cond_pid | cond_bc_paper if category_name == '16ptBusinessCard' else cond_pid); choices.append(category_name)
    
    if conditions: df['Category'] = np.select(conditions, choices, default='Uncategorized')
    else: utils_ui.print_warning("No categories defined in config."); df['Category'] = 'Uncategorized'

    # --- Phase 1.5: Blank ID Rescue ---
    # utils_ui.print_info("Phase 1.5: Rescuing 'Uncategorized' items...")
    uncategorized_mask = (df['Category'] == 'Uncategorized')
    rescue_mask = uncategorized_mask & cond_bc_paper
    num_rescued = rescue_mask.sum()
    if num_rescued > 0:
        df.loc[rescue_mask, 'Category'] = '16ptBusinessCard'
        utils_ui.print_info(f"Rescued {num_rescued} items to '16ptBusinessCard'.")

    # --- Phase 2: Job-Aware Re-categorization Rules ---
    utils_ui.print_info("Phase 2: Job-Aware Rules...")
    job_categories = df.groupby(col_base_job)['Category'].unique(); mixed_jobs = job_categories[job_categories.apply(len) > 1]
    jobs_to_pod = mixed_jobs[mixed_jobs.apply(lambda x: 'PrintOnDemand' in x and 'Uncategorized' not in x)].index
    if len(jobs_to_pod) > 0: 
        utils_ui.print_info(f"Forcing {len(jobs_to_pod)} mixed jobs to 'PrintOnDemand'.")
        df.loc[df[col_base_job].isin(jobs_to_pod), 'Category'] = 'PrintOnDemand'
    
    qty_threshold = config.get('categorization_rules', {}).get('high_quantity_threshold', float('inf'))
    if isinstance(qty_threshold, (int, float)) and col_qty in df.columns and col_base_job in df.columns:
        high_qty_mask = (df['Category'].isin(['12ptBounceBack', '16ptBusinessCard'])) & (df[col_qty] > qty_threshold)
        if high_qty_mask.any():
            jobs_to_move_qty = df.loc[high_qty_mask, col_base_job].unique()
            if len(jobs_to_move_qty) > 0: 
                utils_ui.print_info(f"Moving {len(jobs_to_move_qty)} high-qty jobs to '25up layout'.")
                df.loc[df[col_base_job].isin(jobs_to_move_qty), 'Category'] = '25up layout'
    
    col_url = config.get('column_names', {}).get('one_up_output_file_url')
    if col_url and col_url in df.columns:
        has_no_url_content = df[col_url].isnull() | (df[col_url].astype(str).str.strip() == '')
        eligible_url_mask = df['Category'].isin(['12ptBounceBack', '16ptBusinessCard']) & has_no_url_content
        if eligible_url_mask.sum() > 0:
            jobs_to_move_url = df.loc[eligible_url_mask, col_base_job].unique()
            if len(jobs_to_move_url) > 0: 
                utils_ui.print_info(f"Moving {len(jobs_to_move_url)} jobs with empty URL to 'PrintOnDemand'.")
                df.loc[df[col_base_job].isin(jobs_to_move_url), 'Category'] = 'PrintOnDemand'

    # --- Handle Uncategorized Items ---
    uncategorized_mask = (df['Category'] == 'Uncategorized')
    num_uncategorized = uncategorized_mask.sum()
    if num_uncategorized > 0:
        utils_ui.print_warning(f"Found {num_uncategorized} Uncategorized rows. Moving to exceptions.")
        exceptions_df = pd.concat([exceptions_df, df[uncategorized_mask]], ignore_index=True)
        df = df[~uncategorized_mask].copy()

    # --- Final Split & Stats ---
    categorized_dfs = {cat: df[df['Category'] == cat].copy() for cat in df['Category'].unique() if cat != 'Uncategorized'}
    initial_stats = {}
    
    utils_ui.print_section("Categorization Results")
    if not categorized_dfs:
        utils_ui.print_warning("No rows remaining after processing.")
    
    for cat, cat_df in categorized_dfs.items():
        stats = {'rows': len(cat_df), 'qty': cat_df[col_qty].sum() if col_qty in cat_df.columns else 0,
                 'orders': cat_df[col_order].nunique() if col_order in cat_df.columns else 0, 'jobs': cat_df[col_base_job].nunique() if col_base_job in cat_df.columns else 0}
        initial_stats[cat] = stats
        utils_ui.print_info(f"{cat:<20} : {stats['rows']:>4} rows | Qty: {int(stats['qty']):,}")

    categorized_dfs['_initial_stats'] = initial_stats
    categorized_dfs['_original_columns'] = original_columns

    return {'categorized': categorized_dfs, 'exceptions': exceptions_df, 'dropped_zero_qty': dropped_zero_qty_df}

# --- Main Function ---
def main(input_excel_path, output_dir, config_path):
    utils_ui.setup_logging(None)
    utils_ui.print_banner("20a - Product Data Sorter")

    central_config = load_config_from_path(config_path)
    start_time = time.time()
    
    base_name_unsorted, file_ext = os.path.splitext(os.path.basename(input_excel_path))

    if base_name_unsorted.endswith("_UNSORTED"):
        base_name_categorized = base_name_unsorted.replace("_UNSORTED", "_CATEGORIZED")
    else:
        utils_ui.print_warning(f"Input file name '{base_name_unsorted}{file_ext}' did not end with '_UNSORTED'.")
        base_name_categorized = f"{base_name_unsorted}_CATEGORIZED"
    
    final_output_path = os.path.join(output_dir, f"{base_name_categorized}{file_ext}")
    # utils_ui.print_info(f"Input: {os.path.basename(input_excel_path)}")
    # utils_ui.print_info(f"Target Output: {os.path.basename(final_output_path)}")

    try:
        organized_data = organize_by_product_id(input_file=input_excel_path, config=central_config)

        if organized_data is None:
            raise Exception("Data organization returned None.")

        # --- Save ---
        utils_ui.print_info("Saving categorized sheets...")
        
        categorized_dfs = organized_data.get('categorized', {})
        exceptions_df = organized_data.get('exceptions', pd.DataFrame())
        dropped_zero_qty_df = organized_data.get('dropped_zero_qty', pd.DataFrame())
        
        # --- Save Dropped Zero Qty Report ---
        if not dropped_zero_qty_df.empty:
            dropped_report_name = f"Dropped_Zero_Qty_Rows_{datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')}.xlsx"
            dropped_report_path = os.path.join(output_dir, dropped_report_name)
            try:
                dropped_zero_qty_df.to_excel(dropped_report_path, index=False)
                utils_ui.print_success(f"Saved Zero-Qty Report: {dropped_report_name}")
            except Exception as e:
                utils_ui.print_error(f"Failed to save Zero-Qty Report: {e}")
        
        original_columns = categorized_dfs.pop('_original_columns', [])
        _ = categorized_dfs.pop('_initial_stats', {})
        
        if not original_columns and not exceptions_df.empty:
            original_columns = exceptions_df.columns.tolist()
        
        helper_cols = ['Base Job Ticket Number', 'job_total_line_items', 'Category']
        for col in helper_cols:
            if col not in original_columns and any(col in df.columns for df in categorized_dfs.values()):
                original_columns.append(col)
            if col not in original_columns and col in exceptions_df.columns:
                original_columns.append(col)

        sheets_to_write = {}
        sheets_to_write.update(categorized_dfs)
        if not exceptions_df.empty:
            sheets_to_write['exceptions'] = exceptions_df
            
        if not sheets_to_write:
            utils_ui.print_warning("No data to write. Creating empty file.")
            pd.DataFrame().to_excel(final_output_path)
        else:
            with pd.ExcelWriter(final_output_path) as writer:
                for sheet_name, df_sheet in sheets_to_write.items():
                    if isinstance(df_sheet, pd.DataFrame):
                        # utils_ui.print_info(f"  - Sheet: '{sheet_name}' ({len(df_sheet)} rows)")
                        df_sheet.reindex(columns=original_columns).to_excel(writer, sheet_name=sheet_name, index=False)
        
        utils_ui.print_success(f"Categorization Complete: {os.path.basename(final_output_path)}")
        
    except Exception as e:
        utils_ui.print_error(f"Processing failed: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        utils_ui.print_success(f"Processing Time: {time.time() - start_time:.2f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="20a - Sort and Categorize data.")
    parser.add_argument("input_excel_path", help="Path to the single input _UNSORTED Excel file.")
    parser.add_argument("output_dir", help="Directory to save the _CATEGORIZED Excel file.")
    parser.add_argument("config_path", help="Path to the central configuration YAML file.")
    args = parser.parse_args()

    main(args.input_excel_path, args.output_dir, args.config_path)