import pandas as pd
import xml.etree.ElementTree as ET
import os
import re
from datetime import timedelta, datetime, date
import yaml

# --- Holiday Logic ---
try:
    import holidays
    from holidays.countries import UnitedStates
except ImportError:
    UnitedStates = None

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
    if isinstance(order_date, datetime) or isinstance(order_date, date): 
        if hasattr(order_date, 'date'): current_date = order_date.date()
        else: current_date = order_date
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

# --- XML Parsing ---

def get_xml_text(element, default=""):
    if element is not None and element.text:
        return element.text.strip()
    return default

def find_tag_text(base, path, default=""):
    node = base.find(path)
    return get_xml_text(node, default)

def parse_orders_xml(xml_path):
    if not os.path.exists(xml_path):
        return pd.DataFrame()
        
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except Exception as e:
        print(f"Error parsing XML {xml_path}: {e}")
        return pd.DataFrame()

    records = []
    order_nodes = root.findall('.//Orders/Order')
    
    for order_container in order_nodes:
        order_headers = order_container.findall('Item')
        for order_header_item in order_headers:
            order_number = find_tag_text(order_header_item, 'OrderNumber')
            order_date_str = find_tag_text(order_header_item, 'CreateDate')
            order_date = pd.NaT
            try:
                if order_date_str: order_date = pd.to_datetime(order_date_str)
            except: pass

            order_details_node = order_header_item.find('OrderDetails')
            if order_details_node is None: continue
            
            line_items = order_details_node.findall('.//OrderDetail/Item')
            for item in line_items:
                # Extract fields
                qty_str = find_tag_text(item, 'Quantity')
                try: quantity = int(float(qty_str)) if qty_str else 0
                except: quantity = 0
                
                # Shipping
                ship_node = item.find('Shipping')
                ship_date_raw = pd.NaT
                ship_data = {}
                if ship_node is not None:
                     s_date = find_tag_text(ship_node, 'Date')
                     if s_date:
                         try: ship_date_raw = pd.to_datetime(s_date)
                         except: pass
                     
                     addr_node = ship_node.find('Address')
                     if addr_node is not None:
                         ship_data = {
                             'attn': find_tag_text(addr_node, 'Attn'),
                             'company': find_tag_text(addr_node, 'CompanyName'),
                             'address1': find_tag_text(addr_node, 'Address1'),
                             'address2': find_tag_text(addr_node, 'Address2'),
                             'address3': find_tag_text(addr_node, 'Address3'),
                             'city': find_tag_text(addr_node, 'City'),
                             'state': find_tag_text(addr_node, 'State'),
                             'zip': find_tag_text(addr_node, 'Zip'),
                             'country': find_tag_text(addr_node, 'Country'),
                             'instructions': find_tag_text(ship_node, 'Instructions')
                         }

                # Cost Center Logic
                cost_center = find_tag_text(item, 'Department/Number')
                if not cost_center: cost_center = find_tag_text(item, 'CostCenter')
                if not cost_center: cost_center = find_tag_text(item, 'Reference1')
                if not cost_center: cost_center = find_tag_text(item, 'StoreNumber')

                # File URL
                file_url = ""
                output_urls = item.findall('OutputFileURL/Item/URL')
                for url_node in output_urls:
                    if url_node is not None and url_node.text:
                        u = url_node.text.strip()
                        if '_defaultImposition_' not in u:
                            file_url = u; break

                record = {
                    'order_number': order_number,
                    'order_date': order_date,
                    'job_ticket_number': find_tag_text(item, 'SupplierWorkOrder/Name'),
                    'order_item_id': find_tag_text(item, 'ID/_value_1'),
                    'product_id': find_tag_text(item, 'ProductID/_value_1'),
                    'product_name': find_tag_text(item, 'ProductName'),
                    'product_description': find_tag_text(item, 'ProductDescription'),
                    'sku': find_tag_text(item, 'SKU/Name'),
                    'sku_description': find_tag_text(item, 'SKUDescription'),
                    'quantity_ordered': quantity,
                    'cost_center': cost_center,
                    'ship_date_raw': ship_date_raw,
                    'file_url': file_url,
                    'ship_to_company': ship_data.get('company', ''),
                    'ship_to_name': ship_data.get('attn', ''),
                    'address1': ship_data.get('address1', ''),
                    'address2': ship_data.get('address2', ''),
                    'address3': ship_data.get('address3', ''),
                    'city': ship_data.get('city', ''),
                    'state': ship_data.get('state', ''),
                    'zip': ship_data.get('zip', ''),
                    'country': ship_data.get('country', ''),
                    'shipping_instructions': ship_data.get('instructions', '')
                }
                records.append(record)
    
    return pd.DataFrame(records)

def parse_job_tickets_xml(xml_path):
    if not os.path.exists(xml_path): return pd.DataFrame()
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except: return pd.DataFrame()

    items_node = root.find('Items')
    if items_node is None:
         for child in root:
             if child.tag.endswith('Items'): items_node = child; break
    if items_node is None: return pd.DataFrame()

    records = []
    import datetime as dt_module
    eval_context = {
        'datetime': dt_module,
        'date': date,
        'timedelta': timedelta,
        'True': True, 'False': False, 'None': None
    }
    
    for item in items_node:
        try:
            content = item.text
            if not content:
                content = ET.tostring(item, encoding='unicode', method='text').strip()
            if not content: continue
            
            data_dict = eval(content, {"__builtins__": {}}, eval_context)
            
            instructions = data_dict.get('JobTicketInstructions', {})
            if instructions is None: instructions = {}
            
            record = {
                'job_ticket_number': data_dict.get('JobTicketNumber'),
                'project_description': data_dict.get('ProjectDescription'),
                'general_description': instructions.get('GeneralDescription'),
                'paper_description': instructions.get('PaperDescription'),
                'press_instructions': instructions.get('PressInstructions'),
                'bindery_instructions': instructions.get('BinderyInstructions'),
                'job_ticket_shipping_instructions': instructions.get('ShippingInstructions')
            }
            records.append(record)
        except: pass
        
    return pd.DataFrame(records)

def calculate_box_requirements(df, config):
    # (Same logic as 10_DataCollection)
    if 'order_item_id' not in df.columns or 'product_id' not in df.columns or 'quantity_ordered' not in df.columns:
        return df

    product_ids_map = config.get('product_ids', {})
    rules_map = config.get('shipping_box_rules', {})
    remapping_map = config.get('product_id_remapping', {})
    
    pid_to_cat = {}
    for cat, pids in product_ids_map.items():
        if isinstance(pids, list):
            for pid in pids: pid_to_cat[str(pid)] = cat

    box_data = {f'box_{chr(65+i)}': [] for i in range(8)}

    for idx, row in df.iterrows():
        raw_pid = str(row.get('product_id', '')).split('.')[0].strip()
        qty = str(int(row.get('quantity_ordered', 0)))
        
        # Apply remapping first to handle legacy string IDs like T_TY_FA_BBK
        remapped_pid = str(remapping_map.get(raw_pid, raw_pid))
        
        category = pid_to_cat.get(remapped_pid)
        if not category:
             paper_desc = str(row.get('paper_description', ''))
             if '16pt' in paper_desc.lower() or '16 pt' in paper_desc.lower():
                 category = '16ptBusinessCard'
        
        seq = []
        if category:
            cat_rules = rules_map.get(category, {})
            qty_rule = cat_rules.get(qty)
            if qty_rule: seq = qty_rule.get('box_sequence', [])

        order_item_id_str = str(row.get('order_item_id', '')).replace('<NA>', '').replace('nan', '')
        
        for i in range(8):
            col_name = f'box_{chr(65+i)}'
            if i < len(seq) and order_item_id_str:
                suffix = chr(65+i)
                barcode_val = f"{order_item_id_str}{suffix}"
                box_data[col_name].append(barcode_val)
            else:
                box_data[col_name].append(None)

    for col, data in box_data.items():
        df[col] = data
    return df
