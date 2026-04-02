import os
import sys
import yaml
import json

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

from shared_lib.database import get_db_connection, init_db

def seed_database():
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to database.")
        return

    # Ensure tables exist
    init_db(conn)

    config_path = os.path.join(project_root, 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    cur = conn.cursor()

    try:
        # Seed global settings
        # Seed global settings
        print("Seeding global settings...")
        ignored_keys = ['product_ids', 'standing_files', 'shipping_box_rules', 'run_history']
        
        for k, v in config.items():
            if k not in ignored_keys:
                val_json = json.dumps(v)
                cur.execute("""
                    INSERT INTO global_settings (key, value) 
                    VALUES (%s, %s::jsonb)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """, (k, val_json))

        # Seed categories (derived from product_ids and shipping rules)
        print("Seeding categories...")
        categories = set(config.get('product_ids', {}).keys())
        for cat in categories:
            cur.execute("""
                INSERT INTO production_categories (name) 
                VALUES (%s) 
                ON CONFLICT (name) DO NOTHING
            """, (cat,))

        # Seed products
        print("Seeding products and standing files...")
        product_ids = config.get('product_ids', {})
        standing_files = config.get('standing_files', {})
        
        for category_name, marcom_items in product_ids.items():
            for item in marcom_items:
                marcom_id_str = str(item)
                s_file = standing_files.get(marcom_id_str)
                
                cur.execute("""
                    INSERT INTO app_products (marcom_id, category_name, standing_file)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (marcom_id) DO UPDATE SET 
                        category_name = EXCLUDED.category_name,
                        standing_file = EXCLUDED.standing_file
                """, (marcom_id_str, category_name, s_file))

        # Seed shipping box rules
        print("Seeding shipping box rules...")
        box_rules = config.get('shipping_box_rules', {})
        for rule_id, quantities in box_rules.items():
            for qty_str, stats in quantities.items():
                qty = int(qty_str)
                icon_file = stats.get('icon_file')
                icon_cards = stats.get('icon_cards')
                extra_blanks = stats.get('extra_blanks')
                total_segments = stats.get('total_segments')
                stack_cards = stats.get('stack_cards')
                box_sq = stats.get('box_sequence')
                box_sq_json = json.dumps(box_sq) if box_sq else None

                cur.execute("""
                    INSERT INTO shipping_box_rules 
                    (rule_identifier, quantity, icon_file, icon_cards, extra_blanks, total_segments, stack_cards, box_sequence)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (rule_identifier, quantity) DO UPDATE SET
                        icon_file = EXCLUDED.icon_file,
                        icon_cards = EXCLUDED.icon_cards,
                        extra_blanks = EXCLUDED.extra_blanks,
                        total_segments = EXCLUDED.total_segments,
                        stack_cards = EXCLUDED.stack_cards,
                        box_sequence = EXCLUDED.box_sequence
                """, (rule_id, qty, icon_file, icon_cards, extra_blanks, total_segments, stack_cards, box_sq_json))

        conn.commit()
        print("Database seeded successfully!")
    
    except Exception as e:
        conn.rollback()
        print(f"Error seeding database: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    seed_database()
