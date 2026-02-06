# 10_DataCollection.py
# --- Dependencies and Setup ---
import pandas as pd
import numpy as np
import datetime
import datetime as dt_module # For eval context
import os
import shutil
import traceback
import glob 
import sys
import json               
import argparse
import re           
import xml.etree.ElementTree as ET
import ast
from typing import List, Dict, Tuple, Any, Optional
from datetime import datetime, timedelta, date

import utils_ui 
import yaml # Added for config loading

# --- Configuration Loading ---
def load_config(config_path=None):
    if config_path is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        config_path = os.path.join(project_root, 'config', 'config.yaml')

    if not os.path.exists(config_path):
        # Fallback or error? For data collection mapping, it might be critical now.
        utils_ui.print_warning(f"Config file not found at {config_path}. Box logic may be skipped.")
        return {}
    try:
        with open(config_path, 'r') as f: return yaml.safe_load(f)
    except Exception as e:
        utils_ui.print_error(f"Failed to load config: {e}")
        return {}

# --- Holiday Libraries ---
try:
    import holidays
    from holidays.countries import UnitedStates
except ImportError:
    try: 
        utils_ui.print_warning("Library 'holidays' not found. Ship date calc will ignore holidays.")
        UnitedStates = None
    except:
        pass

# --- Ship Date Calculation Helpers ---
class CustomUS(UnitedStates if UnitedStates else object):
    def _populate(self, year):
        if UnitedStates:
            super()._populate(year)
            thanksgiving_date = None
            for date_obj, name in self.items():
                if name == "Thanksgiving":
                    thanksgiving_date = date_obj
                    break
            if thanksgiving_date:
                self[thanksgiving_date + timedelta(days=1)] = "Day after Thanksgiving"

def calculate_ship_date(order_date, lead_time_days=5):
    if pd.isna(order_date): return pd.NaT
    current_date = None
    if isinstance(order_date, datetime): current_date = order_date.date()
    else:
        try: current_date = pd.Timestamp(order_date).date()
        except Exception: return pd.NaT
    if current_date is None: return pd.NaT
    
    if UnitedStates:
        us_holidays = CustomUS(observed=True, years=current_date.year)
        ship_date_calc = current_date + timedelta(days=lead_time_days)
        while ship_date_calc.weekday() >= 5 or ship_date_calc in us_holidays:
            ship_date_calc += timedelta(days=1)
            current_holiday_years = getattr(us_holidays, '_years', getattr(us_holidays, 'years', [0]))
            if isinstance(current_holiday_years, list): current_holiday_years = set(current_holiday_years)
            if ship_date_calc.year not in current_holiday_years:
                 us_holidays = CustomUS(observed=True, years=ship_date_calc.year)
        return pd.Timestamp(ship_date_calc)
    else:
        ship_date_calc = current_date + timedelta(days=lead_time_days)
        while ship_date_calc.weekday() >= 5:
            ship_date_calc += timedelta(days=1)
        return pd.Timestamp(ship_date_calc)


# --- Column Order Configuration ---
PREFERRED_COLUMN_ORDER = [
    'job_ticket_number', 'product_id', 'quantity_ordered', 'order_number', 'order_item_id',
    'order_date', 'ship_date',
    'cost_center', 'sku', 'ship_to_name', 'ship_attn', 'ship_to_company',
    'address1', 'address2', 'address3', 'address4', 'city', 'state', 'zip', 'country',
    'special_instructions', 'product_name', 'general_description', 'paper_description',
    'press_instructions', 'bindery_instructions', 'job_ticket_shipping_instructions',
    'sku_description', 'product_description', '1-up_output_file_url'
]

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = []
    for col in df.columns:
        new_col = str(col).lower().strip().replace(' ', '_').replace('#', 'num')
        new_cols.append(new_col)
    df.columns = new_cols
    return df

# --- XML Parsing Logic ---

def get_xml_text(element: Optional[ET.Element], default: str = "") -> str:
    if element is not None and element.text:
        return element.text.strip()
    return default

def find_tag_text(base: ET.Element, path: str, default: str = "") -> str:
    node = base.find(path)
    return get_xml_text(node, default)

def parse_orders_xml(xml_path: str) -> pd.DataFrame:
    if not os.path.exists(xml_path):
        raise FileNotFoundError(f"Orders XML not found: {xml_path}")

    utils_ui.print_info(f"Parsing Orders XML: {os.path.basename(xml_path)}")
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except Exception as e:
        utils_ui.print_error(f"Failed to parse XML: {e}")
        raise

    # Robust iterating over Orders
    order_nodes = root.findall('.//Orders/Order')
    utils_ui.print_info(f"Found {len(order_nodes)} 'Order' container blocks.")

    records = []

    for order_container in order_nodes:
        order_headers = order_container.findall('Item')
        utils_ui.print_info(f"Found {len(order_headers)} Order Header Items in container.")
        
        for order_header_item in order_headers:
            order_number = find_tag_text(order_header_item, 'OrderNumber')
            order_date_str = find_tag_text(order_header_item, 'CreateDate')
            
            order_date = pd.NaT
            try:
                if order_date_str:
                    order_date = pd.to_datetime(order_date_str).normalize()
            except:
                pass

            order_details_node = order_header_item.find('OrderDetails')
            if order_details_node is None: continue
            
            line_items = order_details_node.findall('.//OrderDetail/Item')
            
            for item in line_items:
                order_item_id = find_tag_text(item, 'ID/_value_1')
                job_ticket_number = find_tag_text(item, 'SupplierWorkOrder/Name')
                cost_center = find_tag_text(item, 'Department/Number')
                if not cost_center:
                    cost_center = find_tag_text(item, 'CostCenter')
                if not cost_center:
                    # Fallback to Reference1 (seen in some Marcom formats)
                    cost_center = find_tag_text(item, 'Reference1')
                if not cost_center:
                     # Check Custom Fields if structured that way (common in Marcom)
                     # But without schema, we stick to common names
                     cost_center = find_tag_text(item, 'StoreNumber')
                
                product_id = find_tag_text(item, 'ProductID/_value_1')
                product_name = find_tag_text(item, 'ProductName')
                product_desc = find_tag_text(item, 'ProductDescription')
                sku = find_tag_text(item, 'SKU/Name')
                sku_desc = find_tag_text(item, 'SKUDescription')
                
                qty_str = find_tag_text(item, 'Quantity')
                try: quantity = int(float(qty_str)) if qty_str else 0
                except: quantity = 0
                    
                ship_node = item.find('Shipping')
                ship_date_raw = pd.NaT
                ship_to_name = ""
                ship_attn = ""
                ship_company = ""
                addr1, addr2, addr3, city, state, zip_code, country, shipping_instr = "","","","","","","",""
                
                if ship_node is not None:
                    s_date = find_tag_text(ship_node, 'Date')
                    if s_date:
                        try: ship_date_raw = pd.to_datetime(s_date)
                        except: pass
                    
                    shipping_instr = find_tag_text(ship_node, 'Instructions')
                    
                    addr_node = ship_node.find('Address')
                    if addr_node is not None:
                        ship_attn = find_tag_text(addr_node, 'Attn')
                        ship_to_name = ship_attn
                        ship_company = find_tag_text(addr_node, 'CompanyName')
                        addr1 = find_tag_text(addr_node, 'Address1')
                        addr2 = find_tag_text(addr_node, 'Address2')
                        addr3 = find_tag_text(addr_node, 'Address3')
                        city = find_tag_text(addr_node, 'City')
                        state = find_tag_text(addr_node, 'State')
                        zip_code = find_tag_text(addr_node, 'Zip')
                        country = find_tag_text(addr_node, 'Country')
                
                file_url = ""
                # Priority: OutputFileURL that does NOT contain '_defaultImposition_'
                output_urls = item.findall('OutputFileURL/Item/URL')
                for url_node in output_urls:
                    if url_node is not None and url_node.text:
                        u = url_node.text.strip()
                        if '_defaultImposition_' not in u:
                            file_url = u
                            break
                
                # Fallback: if no clean OutputFileURL, try ImposedUsingDefaultImpo IF allowed?
                # User said: "urls... found under <ImposedUsingDefaultImpo> are NOT what we need."
                # So we won't fallback to that.
                
                # If file_url is still empty, maybe there is only a defaultImposition one?
                # For now, strict adherence to user request.

                record = {
                    'job_ticket_number': job_ticket_number,
                    'product_id': product_id,
                    'quantity_ordered': quantity,
                    'order_number': order_number,
                    'order_item_id': order_item_id,
                    'order_date': order_date,
                    'ship_date': ship_date_raw,
                    'cost_center': cost_center,
                    'sku': sku,
                    'ship_to_name': ship_to_name,
                    'ship_attn': ship_attn,
                    'ship_to_company': ship_company,
                    'address1': addr1,
                    'address2': addr2,
                    'address3': addr3,
                    'address4': '',
                    'city': city,
                    'state': state,
                    'zip': zip_code,
                    'country': country,
                    'special_instructions': '', 
                    'product_name': product_name,
                    'general_description': '', 
                    'paper_description': '', 
                    'press_instructions': '',
                    'bindery_instructions': '',
                    'job_ticket_shipping_instructions': shipping_instr,
                    'sku_description': sku_desc,
                    'product_description': product_desc,
                    '1-up_output_file_url': file_url
                }
                records.append(record)

    df = pd.DataFrame(records)
    if 'order_item_id' in df.columns:
        df['order_item_id'] = pd.to_numeric(df['order_item_id'], errors='coerce').astype('Int64')
        
    utils_ui.print_info(f"Extracted {len(df)} records from Orders XML.")
    return df

def parse_job_tickets_xml(xml_path: str) -> pd.DataFrame:
    if not os.path.exists(xml_path):
        utils_ui.print_warning(f"JobTickets XML not found: {xml_path}")
        return pd.DataFrame()

    utils_ui.print_info(f"Parsing JobTickets XML: {os.path.basename(xml_path)}")
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except Exception as e:
        utils_ui.print_error(f"Failed to parse JobTickets XML: {e}")
        return pd.DataFrame()

    # Debug root
    # print(f"DEBUG: Root tag is {root.tag}")
    
    # Try finding Items with flexible search
    items_node = root.find('Items')
    if items_node is None:
        # Namespace fallback or recursive search
        for child in root:
            if child.tag.endswith('Items'):
                items_node = child
                break
                
    if items_node is None:
        utils_ui.print_warning("Could not find 'Items' node in JobTickets XML.")
        return pd.DataFrame()

    utils_ui.print_info(f"Found {len(items_node)} items in JobTickets.")

    records = []
    
    # Context for eval
    eval_context = {
        'datetime': dt_module,
        'date': date,
        'timedelta': timedelta,
        'True': True,
        'False': False,
        'None': None
    }

    for i, item in enumerate(items_node):
        try:
            content = item.text
            if not content:
                # Fallback: maybe content is tail? or mixed?
                # Sometimes ElementTree puts text in .text, sometimes not if there are children.
                # If the indentation makes it look like children...
                # Get strict string content:
                content = ET.tostring(item, encoding='unicode', method='text').strip()
            
            if not content: continue
            
            # If tostring includes the tag text itself if not careful, but method='text' strips tags.
            # But wait, the content IS python code. 
            # If ElementTree parsed <ID> as a child tag inside Item_0, then 'text' property is fragmented.
            # We want the ORIGINAL RAW text.
            # Reconstruct it from all text parts?
            
            # Simple approach: If valid python dict structure is preserved in text nodes.
            # Let's hope item.text is enough, or just use the first chunk if it's a dict.
            
            # Clean content if needed (e.g. remove outer braces if they are missing?)
            
            data_dict = eval(content, {"__builtins__": {}}, eval_context)
            
            jt_num = data_dict.get('JobTicketNumber')
            proj_desc = data_dict.get('ProjectDescription')
            
            # Extract detailed instructions from nested dictionary
            instructions_dict = data_dict.get('JobTicketInstructions', {})
            if instructions_dict is None: instructions_dict = {}

            record = {
                'job_ticket_number': jt_num,
                'job_ticket_project_description': proj_desc,
                'general_description': instructions_dict.get('GeneralDescription'),
                'paper_description': instructions_dict.get('PaperDescription'),
                'press_instructions': instructions_dict.get('PressInstructions'),
                'bindery_instructions': instructions_dict.get('BinderyInstructions'),
                'job_ticket_shipping_instructions': instructions_dict.get('ShippingInstructions')
            }
            records.append(record)
        except Exception as ex:
            if i < 3:
                utils_ui.print_error(f"Item {i} parse error: {ex}")
                # print(f"DEBUG CONTENT: {content[:100]}...")
            pass

    df = pd.DataFrame(records)
    utils_ui.print_info(f"Extracted {len(df)} records from JobTickets XML.")
    return df

def calculate_box_requirements(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Dynamically generates box barcode columns (box_A, box_B, etc.) based on
    product ID categorization and quantity packing rules defined in config.
    """
    if 'order_item_id' not in df.columns or 'product_id' not in df.columns or 'quantity_ordered' not in df.columns:
        return df

    # Prepare configuration lookups
    product_ids_map = config.get('product_ids', {})
    rules_map = config.get('shipping_box_rules', {})
    
    # 1. Map Product IDs to Categories
    # Invert the map: product_id -> category_name
    pid_to_cat = {}
    for cat, pids in product_ids_map.items():
        if isinstance(pids, list):
            for pid in pids:
                pid_to_cat[str(pid)] = cat

    # Helper to determine boxes needed
    def get_box_sequence(row):
        pid = str(row.get('product_id', '')).split('.')[0].strip()
        qty = str(int(row.get('quantity_ordered', 0))) # Key is string in yaml
        
        category = pid_to_cat.get(pid)
        
        # Checking for Business Card override (standard logic from script 20)
        # Not strictly needed if PID map is complete, but good for safety
        if not category:
             paper_desc = str(row.get('paper_description', ''))
             if '16pt' in paper_desc.lower() or '16 pt' in paper_desc.lower():
                 category = '16ptBusinessCard'
        
        if not category: return []

        # Look up rule
        cat_rules = rules_map.get(category, {})
        qty_rule = cat_rules.get(qty)
        
        if qty_rule:
            return qty_rule.get('box_sequence', [])
        
        return []

    # 2. Iterate and Populate
    # We'll use a dictionary of lists to build the new columns
    box_data = {f'box_{chr(65+i)}': [] for i in range(8)} # box_A ... box_H

    for idx, row in df.iterrows():
        seq = get_box_sequence(row)
        order_item_id_str = str(row.get('order_item_id', '')).replace('<NA>', '').replace('nan', '')
        
        for i in range(8):
            col_name = f'box_{chr(65+i)}'
            if i < len(seq) and order_item_id_str:
                # Generate barcode: ID + Suffix (A, B, C...)
                suffix = chr(65+i)
                barcode_val = f"{order_item_id_str}{suffix}"
                box_data[col_name].append(barcode_val)
            else:
                box_data[col_name].append(None)

    # 3. Assign back to DF
    for col, data in box_data.items():
        df[col] = data
        
    return df

def clean_dataframe_for_output(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()
    for col in PREFERRED_COLUMN_ORDER:
        if col not in df_clean.columns: df_clean[col] = None
    existing = [c for c in PREFERRED_COLUMN_ORDER if c in df_clean.columns]
    other = sorted([c for c in df_clean.columns if c not in existing])
    df_clean = df_clean[existing + other]
    obj_cols = df_clean.select_dtypes(include=['object']).columns
    df_clean[obj_cols] = df_clean[obj_cols].fillna('')
    return df_clean

def generate_and_log_summary(final_report_df: pd.DataFrame, file_path: str, success: bool = True, error_details: str = "") -> None:
    if success:
        msg = f"Consolidation Summary:\n  - Final Report: {os.path.basename(file_path)}\n  - Total Rows: {len(final_report_df)}\n  - Total Columns: {len(final_report_df.columns)}"
        utils_ui.print_success(msg)
    else:
        utils_ui.print_error(f"Consolidation Failed: {error_details}")

# --- Safeguard: Filename Validation ---
def extract_date_range(filename: str) -> Optional[str]:
    """
    Extracts the 'START_to_END' timestamp string from filenames.
    Format: Type_YYYYMMDD_HHMM_to_YYYYMMDD_HHMM_Created.xml
    Regex looks for: YYYYMMDD_HHMM_to_YYYYMMDD_HHMM
    """
    match = re.search(r'(\d{8}_\d{4}_to_\d{8}_\d{4})', filename)
    return match.group(1) if match else None

def validate_file_pairs(orders_paths: List[str], tickets_paths: List[str]) -> None:
    """
    Ensures that for every Order file, there is a Ticket file covering the exact same window.
    Strictly halts on mismatch.
    """
    utils_ui.print_info("Validating input file pairs (Date Range Check)...")
    
    order_ranges = {}
    for p in orders_paths:
        fname = os.path.basename(p)
        rng = extract_date_range(fname)
        if rng: order_ranges[rng] = fname
        
    ticket_ranges = {}
    for p in tickets_paths:
        fname = os.path.basename(p)
        rng = extract_date_range(fname)
        if rng: ticket_ranges[rng] = fname
        
    # Check for mismatches
    o_set = set(order_ranges.keys())
    t_set = set(ticket_ranges.keys())
    
    if o_set != t_set:
        utils_ui.print_banner("CRITICAL ERROR: FILE MISMATCH", "Date Range Validation Failed")
        
        missing_in_tickets = o_set - t_set
        missing_in_orders = t_set - o_set
        
        if missing_in_tickets:
            utils_ui.print_error("The following Date Ranges exist in Orders but are MISSING in JobTickets:")
            for m in missing_in_tickets:
                utils_ui.print_error(f"  Range: {m} (File: {order_ranges[m]})")
                
        if missing_in_orders:
            utils_ui.print_error("The following Date Ranges exist in JobTickets but are MISSING in Orders:")
            for m in missing_in_orders:
                utils_ui.print_error(f"  Range: {m} (File: {ticket_ranges[m]})")
                
        utils_ui.print_error("Input files must be perfectly paired by date range.")
        utils_ui.print_error("Stopping production to prevent data corruption.")
        sys.exit(1)
        
    utils_ui.print_success("File Pair Validation Passed: All input files matched.")

def main(staging_dir: str, file_paths_map: Dict[str, Any], remapping_map: Dict[str, Any] = {}) -> None:
    utils_ui.setup_logging(None) 
    utils_ui.print_banner("10 - Data Collection (XML -> XLSX)")
    try:
        if remapping_map:
            utils_ui.print_info(f"Loaded {len(remapping_map)} product ID remapping rules.")

        # Accept LITERAL LISTS or STRINGS
        orders_input = file_paths_map.get('orders_xml')
        tickets_input = file_paths_map.get('job_tickets_xml')

        # Normalize to lists
        orders_xml_paths = orders_input if isinstance(orders_input, list) else ([orders_input] if orders_input else [])
        tickets_xml_paths = tickets_input if isinstance(tickets_input, list) else ([tickets_input] if tickets_input else [])
        
        if not orders_xml_paths: raise FileNotFoundError("Missing 'orders_xml' path.")

        # --- VALIDATE FILE PAIRS ---
        if orders_xml_paths and tickets_xml_paths:
            validate_file_pairs(orders_xml_paths, tickets_xml_paths)

        # --- Parse & Concat Orders ---
        all_orders_dfs = []
        for path in orders_xml_paths:
            df = parse_orders_xml(path)
            if not df.empty:
                all_orders_dfs.append(df)
        
        if not all_orders_dfs:
            utils_ui.print_warning("No Order records found in any source file.")
            df_orders = pd.DataFrame() # Proper handling if empty
        else:
            df_orders = pd.concat(all_orders_dfs, ignore_index=True)
            utils_ui.print_info(f"Total Consolidated Order Records: {len(df_orders)}")

        # --- Parse & Concat Job Tickets ---
        df_tickets = pd.DataFrame()
        if tickets_xml_paths:
            all_ticket_dfs = []
            for path in tickets_xml_paths:
                df = parse_job_tickets_xml(path)
                if not df.empty:
                    all_ticket_dfs.append(df)
            
            if all_ticket_dfs:
                df_tickets = pd.concat(all_ticket_dfs, ignore_index=True)
                utils_ui.print_info(f"Total Ticket Records (Pre-dedup): {len(df_tickets)}")

                if 'job_ticket_number' in df_tickets.columns:
                    # Identify duplicates (to be dropped)
                    # keep='last' means the last one is False (not a duplicate), others are True
                    duplicates_mask = df_tickets.duplicated(subset=['job_ticket_number'], keep='last')
                    df_dropped = df_tickets[duplicates_mask].copy()
                    
                    if not df_dropped.empty:
                        dropped_count = len(df_dropped)
                        utils_ui.print_warning(f"Found {dropped_count} obsolete ticket revisions (duplicates).")
                        
                        # Save dropped report
                        dropped_report_name = f'Dropped_Duplicate_Tickets_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}.xlsx'
                        dropped_report_path = os.path.join(staging_dir, dropped_report_name)
                        try:
                            df_dropped.to_excel(dropped_report_path, index=False, engine='openpyxl')
                            utils_ui.print_success(f"Saved dropped duplicates report: {dropped_report_name}")
                        except Exception as e:
                            utils_ui.print_error(f"Failed to save dropped duplicates report: {e}")

                        # Actually drop them from the main dataframe
                        df_tickets = df_tickets[~duplicates_mask]
                        utils_ui.print_info(f"Retained {len(df_tickets)} unique tickets after deduplication.")

        # --- CRITICAL SAFEGUARD: Orphaned Data Check ---
        # Ensure every Ticket has a matching Order. If not, STOP immediately.
        if not df_tickets.empty and not df_orders.empty:
            if 'job_ticket_number' in df_tickets.columns and 'job_ticket_number' in df_orders.columns:
                
                # Identify tickets that do NOT exist in the Orders list
                # We use .astype(str) to ensure type matching safety
                valid_tickets = set(df_orders['job_ticket_number'].astype(str))
                
                # Check each ticket
                # Using apply/lambda or isin. isin is faster.
                # Normalized strings for comparison
                df_tickets['temp_jt_match_key'] = df_tickets['job_ticket_number'].astype(str).str.strip()
                df_orders['temp_jt_match_key'] = df_orders['job_ticket_number'].astype(str).str.strip()
                
                valid_keys = set(df_orders['temp_jt_match_key'])
                orphans_mask = ~df_tickets['temp_jt_match_key'].isin(valid_keys)
                df_orphans = df_tickets[orphans_mask].copy()
                
                # Cleanup temp cols
                df_tickets.drop(columns=['temp_jt_match_key'], inplace=True)
                df_orders.drop(columns=['temp_jt_match_key'], inplace=True)

                if not df_orphans.empty:
                    orphan_count = len(df_orphans)
                    utils_ui.print_banner("CRITICAL ERROR: DATA CONTAMINATION DETECTED", "Orphaned Job Tickets Found")
                    utils_ui.print_error(f"Found {orphan_count} Job Tickets that do NOT have a matching Order record.")
                    utils_ui.print_error("This indicates a mismatch in input files (e.g. Orders XML vs Tickets XML).")
                    utils_ui.print_error("The script will HALT to prevent data loss/contamination.")
                    
                    # Save Report
                    orphan_report_name = f'CRITICAL_Orphaned_Tickets_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}.xlsx'
                    orphan_report_path = os.path.join(staging_dir, orphan_report_name)
                    try:
                        df_orphans.to_excel(orphan_report_path, index=False, engine='openpyxl')
                        utils_ui.print_warning(f"  -> Details saved to: {orphan_report_path}")
                    except Exception as e:
                        utils_ui.print_error(f"  -> Failed to save orphan report: {e}")
                        
                    sys.exit(1) # STRICT HALT
                else:
                    utils_ui.print_success("Data Integrity Verified: All Job Tickets have matching Orders.")

        # --- Merge Logic (Existing) ---
        if not df_tickets.empty and not df_orders.empty:
             if 'job_ticket_number' in df_tickets.columns and 'job_ticket_number' in df_orders.columns:
                 utils_ui.print_info("Merging JobTickets data...")
                 # Use suffixes to keep both sets of columns
                 df_orders = pd.merge(df_orders, df_tickets, on='job_ticket_number', how='left', suffixes=('', '_ticket'))
                 
                 # Prioritize JobTicket data for instruction columns
                 instruction_cols = [
                     'general_description', 'paper_description', 'press_instructions', 
                     'bindery_instructions', 'job_ticket_shipping_instructions'
                 ]
                 
                 for col in instruction_cols:
                     ticket_col = f"{col}_ticket"
                     if ticket_col in df_orders.columns:
                         # Fill main column with ticket data where available and not empty
                         mask = df_orders[ticket_col].notna() & (df_orders[ticket_col].astype(str).str.strip() != '')
                         df_orders.loc[mask, col] = df_orders.loc[mask, ticket_col]
                         # Drop the temp ticket column
                         df_orders.drop(columns=[ticket_col], inplace=True)

                 if 'job_ticket_project_description' in df_orders.columns:
                     df_orders['special_instructions'] = df_orders['special_instructions'].fillna('')
                     mask = df_orders['special_instructions'] == ''
                     df_orders.loc[mask, 'special_instructions'] = df_orders.loc[mask, 'job_ticket_project_description']
        
        # Apply Product ID Remapping
        if remapping_map and 'product_id' in df_orders.columns:
            # utils_ui.print_info("Applying Product ID remapping...")
            # Use replace strictly or map with fallback? replace is safer to keep originals if not found.
            df_orders['product_id'] = df_orders['product_id'].replace(remapping_map)

        final_df = df_orders.copy()
        if 'job_ticket_number' in final_df.columns and 'sku' in final_df.columns:
            final_df = final_df.sort_values(by=['job_ticket_number', 'sku'], ascending=True)

        if 'job_ticket_number' in final_df.columns and 'sku' in final_df.columns:
            final_df = final_df.sort_values(by=['job_ticket_number', 'sku'], ascending=True)

        # Dynamic Box Calculation
        # Load config (assuming default location ./config.yaml if not passed, but we are in root usually)
        # Ideally, main should get it.
        config = load_config() 
        final_df = calculate_box_requirements(final_df, config)
        output_file_name = f'Consolidated_Report_{datetime.now().strftime("%Y-%m-%d")}.xlsx'
        
        if 'order_date' in final_df.columns:
            utils_ui.print_info("Recalculating ship dates...")
            final_df['order_date'] = pd.to_datetime(final_df['order_date'], errors='coerce')
            final_df['ship_date'] = final_df['order_date'].apply(calculate_ship_date)
            order_dates = final_df['order_date'].dt.date.dropna()
            if not order_dates.empty:
                min_str, max_str = order_dates.min().strftime('%Y-%m-%d'), order_dates.max().strftime('%Y-%m-%d')
                output_file_name = f'MarcomOrderDate {min_str}.xlsx' if min_str == max_str else f'MarcomOrderDate {min_str}_to_{max_str}.xlsx'

        output_file_path = os.path.join(staging_dir, output_file_name)
        cleaned_report_df = clean_dataframe_for_output(final_df)
        cleaned_report_df.to_excel(output_file_path, index=False, engine='openpyxl')
        utils_ui.print_success(f"Created Report: {os.path.basename(output_file_path)}")

        utils_ui.print_info("Moving source files to staging...")
        
        # Flatten source list for moving
        all_source_paths = orders_xml_paths + tickets_xml_paths
        
        for source_path in all_source_paths: 
             if os.path.exists(source_path):
                 shutil.move(source_path, os.path.join(staging_dir, os.path.basename(source_path)))

        generate_and_log_summary(cleaned_report_df, output_file_path, success=True)
        utils_ui.print_success("Stage 1 Complete (XML Mode).")

    except Exception as e:
        utils_ui.print_error(f"Unexpected Error: {e}")
        # print(traceback.format_exc())
        generate_and_log_summary(pd.DataFrame(), "", success=False, error_details=str(e))
        sys.exit(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("staging_dir")
    parser.add_argument("file_paths_map_json")
    parser.add_argument("remapping_json", nargs='?', default="{}")
    args = parser.parse_args()
    try: 
        file_paths_map_dict = json.loads(args.file_paths_map_json)
        remapping_dict = json.loads(args.remapping_json)
    except: sys.exit(1)
    main(args.staging_dir, file_paths_map_dict, remapping_dict)
