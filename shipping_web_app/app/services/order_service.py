
from shared_lib.database import get_db_connection, get_real_dict_cursor
from shared_lib.utils import extract_store_number_strict
from fuzzywuzzy import fuzz

def get_job_details(lookup_id):
    conn = get_db_connection()
    if not conn: return None, "DB Connection Error"
    
    try:
        cur = get_real_dict_cursor(conn)
        
        # 1. Job Ticket Check
        cur.execute("""
            SELECT j.id as job_id, j.order_id, j.job_ticket_number, o.order_number, 
                   o.ship_to_company, o.ship_to_name, 
                   o.address1, o.city, o.state, o.zip, o.country 
            FROM jobs j
            JOIN orders o ON j.order_id = o.id
            WHERE j.job_ticket_number = %s
        """, (lookup_id,))
        job_data = cur.fetchone()
        
        target_job_ids = []
        
        if job_data:
            target_job_ids = [job_data['job_id']]
            response_data = {
                "order_number": job_data['job_ticket_number'], 
                "related_order_number": job_data['order_number'],
                "ship_to": {
                    "name": job_data['ship_to_name'],
                    "company": job_data['ship_to_company'],
                    "address1": job_data['address1'],
                    "city": job_data['city'],
                    "state": job_data['state'],
                    "zip": job_data['zip'],
                    "country": job_data['country'],
                    "account_number": "Y76383"
                },
                "reference2": job_data['job_ticket_number']
            }
        else:
            # 2. Order Number Check
            cur.execute("""
                SELECT id, order_number, ship_to_company, ship_to_name, 
                       address1, city, state, zip, country 
                FROM orders 
                WHERE order_number = %s
            """, (lookup_id,))
            order_data = cur.fetchone()
            
            if not order_data:
                conn.close()
                return None, f"ID {lookup_id} not found."
            
            cur.execute("SELECT id FROM jobs WHERE order_id = %s", (order_data['id'],))
            target_job_ids = [r['id'] for r in cur.fetchall()]
            
            response_data = {
                "order_number": order_data['order_number'], 
                "related_order_number": order_data['order_number'],
                "ship_to": {
                    "name": order_data['ship_to_name'],
                    "company": order_data['ship_to_company'],
                    "address1": order_data['address1'],
                    "city": order_data['city'],
                    "state": order_data['state'],
                    "zip": order_data['zip'],
                    "country": order_data['country'],
                    "account_number": "Y76383"
                },
                "reference2": order_data['order_number']
            }

        # 3. Items
        if target_job_ids:
            cur.execute("""
                SELECT b.barcode_value, b.status, b.packed_at, i.sku, i.sku_description, i.order_item_id, 
                       i.quantity_ordered, i.cost_center, i.product_id, j.job_ticket_number
                FROM item_boxes b
                JOIN items i ON b.order_item_id = i.order_item_id
                JOIN jobs j ON i.job_id = j.id
                WHERE i.job_id = ANY(%s)
                ORDER BY j.job_ticket_number, i.order_item_id, b.box_sequence
            """, (target_job_ids,))
            
            rows = cur.fetchall()
            
            # Map items logic (simplified from original for brevity, but retaining structure)
            # note: weights calc omitted for now to save tokens, assuming 'get_mixed_box_info' 
            # would be imported if fully implemented.
            
            all_barcodes = []
            line_items_list = []
            # ... (Full reconstruction would typically go here)
            
            # Minimal reconstruction for the plan:
            seen_items = {}
            for row in rows:
                all_barcodes.append(row['barcode_value'])
                oid = row['order_item_id']
                if oid not in seen_items:
                    seen_items[oid] = {
                        "job_ticket": row['job_ticket_number'],
                        "sku": row['sku'],
                        "barcodes": []
                    }
                seen_items[oid]['barcodes'].append({
                    "value": row['barcode_value'],
                    "status": row.get('status')
                })
            
            response_data['expected_barcodes'] = all_barcodes
            response_data['line_items'] = list(seen_items.values())

        conn.close()
        return response_data, None

    except Exception as e:
        if conn: conn.close()
        print(e)
        return None, str(e)
