import os
import yaml
from dotenv import load_dotenv

# Load env immediately upon import? Or explicit init?
# Better to have explicit init or load once.
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

def get_env_var(key, default=None):
    return os.getenv(key, default)

import json

def load_yaml_config(config_path=None):
    from .database import get_db_connection, get_real_dict_cursor
    
    if config_path is None:
        # Default to ../config/config.yaml
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(base_dir, 'config', 'config.yaml')
    
    config = {}
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f) or {}

    conn = None
    try:
        conn = get_db_connection()
        if conn:
            cur = get_real_dict_cursor(conn)
            
            # Hydrate Global Settings (JSONB)
            cur.execute("SELECT key, value FROM global_settings")
            globals_rows = cur.fetchall()
            for r in globals_rows:
                k, val = r['key'], r['value']
                # psycopg2 natively returns dicts for JSONB, but fallback if string
                config[k] = val if not isinstance(val, str) else json.loads(val)
                
            # Hydrate Shipping Box Rules
            cur.execute("SELECT * FROM shipping_box_rules")
            box_rows = cur.fetchall()
            if box_rows:
                db_shipping_rules = {}
                for row in box_rows:
                    rule_id = row['rule_identifier']
                    qty_str = str(row['quantity'])
                    if rule_id not in db_shipping_rules:
                        db_shipping_rules[rule_id] = {}
                    db_shipping_rules[rule_id][qty_str] = {
                        'icon_file': row['icon_file'],
                        'icon_cards': row['icon_cards'],
                        'extra_blanks': row['extra_blanks'],
                        'total_segments': row['total_segments'],
                        'stack_cards': row['stack_cards'],
                        'box_sequence': row['box_sequence']
                    }
                config['shipping_box_rules'] = db_shipping_rules

            # Hydrate App Products (product_ids and standing_files)
            cur.execute("SELECT * FROM app_products WHERE category_name IS NOT NULL")
            product_rows = cur.fetchall()
            
            if product_rows:
                db_product_ids = {}
                db_standing_files = {}
                for row in product_rows:
                    cat = row['category_name']
                    m_id = str(row['marcom_id'])
                    
                    if cat not in db_product_ids:
                        db_product_ids[cat] = []
                    db_product_ids[cat].append(m_id)
                    
                    s_file = row['standing_file']
                    if s_file:
                        db_standing_files[m_id] = s_file
                        
                config['product_ids'] = db_product_ids
                if db_standing_files:
                    config['standing_files'] = db_standing_files

            cur.close()
    except Exception as e:
        print(f"Warning: Failed to hydrate configuration from Postgres Database: {e}")
    finally:
        if conn:
            conn.close()

    return config

def get_run_history():
    from .database import get_db_connection, get_real_dict_cursor
    is_dry = os.environ.get("PRODUCTION_DRY_RUN") == "1"
    
    if is_dry:
        default_history = {'monthly_pace_job_number': "987654 TEST", 'last_used_gang_run_suffix': 556}
    else:
        default_history = {'monthly_pace_job_number': 100000, 'last_used_gang_run_suffix': 0}
        
    conn = None
    try:
        conn = get_db_connection()
        if conn:
            cur = get_real_dict_cursor(conn)
            cur.execute("SELECT value FROM global_settings WHERE key = 'run_history'")
            row = cur.fetchone()
            if row:
                val = row['value']
                hist = val if not isinstance(val, str) else json.loads(val)
                
                res = {}
                if is_dry:
                    res['monthly_pace_job_number'] = hist.get('dry_run_monthly_pace_job_number', default_history['monthly_pace_job_number'])
                    res['last_used_gang_run_suffix'] = hist.get('dry_run_last_used_gang_run_suffix', default_history['last_used_gang_run_suffix'])
                else:
                    res['monthly_pace_job_number'] = hist.get('monthly_pace_job_number', default_history['monthly_pace_job_number'])
                    res['last_used_gang_run_suffix'] = hist.get('last_used_gang_run_suffix', default_history['last_used_gang_run_suffix'])
                return res
            cur.close()
    except Exception as e:
        print(f"Failed to fetch run history from DB: {e}")
    finally:
        if conn: conn.close()
    return default_history

def update_run_history(pace_number, last_suffix):
    from .database import get_db_connection, get_real_dict_cursor
    is_dry = os.environ.get("PRODUCTION_DRY_RUN") == "1"
    conn = None
    try:
        conn = get_db_connection()
        if conn:
            cur = get_real_dict_cursor(conn)
            
            # Fetch existing history to merge with
            cur.execute("SELECT value FROM global_settings WHERE key = 'run_history'")
            row = cur.fetchone()
            current_hist = {}
            if row:
                val = row['value']
                current_hist = val if not isinstance(val, str) else json.loads(val)
                
            # Perform targeted updates
            if is_dry:
                current_hist['dry_run_monthly_pace_job_number'] = pace_number
                current_hist['dry_run_last_used_gang_run_suffix'] = last_suffix
            else:
                current_hist['monthly_pace_job_number'] = pace_number
                current_hist['last_used_gang_run_suffix'] = last_suffix
                
            history_json = json.dumps(current_hist)

            cur_write = conn.cursor()
            cur_write.execute("""
                INSERT INTO global_settings (key, value) 
                VALUES ('run_history', %s::jsonb)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """, (history_json,))
            conn.commit()
            cur_write.close()
            cur.close()
    except Exception as e:
        print(f"Failed to update run history in DB: {e}")
    finally:
        if conn: conn.close()
