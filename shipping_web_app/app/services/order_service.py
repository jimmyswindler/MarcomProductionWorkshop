
from shared_lib.database import get_db_connection, get_real_dict_cursor
from shared_lib.utils import extract_store_number_strict, get_product_category
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
                   o.address1, o.city, o.state, o.zip, o.country, o.store_number 
            FROM jobs j
            JOIN orders o ON j.order_id = o.id
            WHERE j.job_ticket_number = %s
        """, (lookup_id,))
        job_data = cur.fetchone()
        
        target_job_ids = []
        
        if job_data:
            # Instead of fetching all jobs for the order_id, only fetch the matched job
            target_job_ids = [job_data['job_id']]
            
            response_data = {
                "order_number": job_data['job_ticket_number'], 
                "related_order_number": job_data['order_number'],
                "searched_job_ticket": job_data['job_ticket_number'],
                "ship_to": {
                    "name": job_data['ship_to_name'],
                    "company": job_data['ship_to_company'],
                    "address1": job_data['address1'],
                    "city": job_data['city'],
                    "state": job_data['state'],
                    "zip": job_data['zip'],
                    "country": job_data['country'],
                    "store_number": str(job_data['store_number']).strip().zfill(4) if job_data.get('store_number') else None,
                    "account_number": "Y76383"
                },
                "reference2": job_data['job_ticket_number']
            }
        else:
            # 2. Order Number Check and Partial Match Fallback
            cur.execute("""
                SELECT id, order_number, ship_to_company, ship_to_name, 
                       address1, city, state, zip, country, store_number 
                FROM orders 
                WHERE order_number = %s
            """, (lookup_id,))
            order_data = cur.fetchone()
            
            if order_data:
                # Case A: Exact Order Match
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
                        "store_number": str(order_data['store_number']).strip().zfill(4) if order_data.get('store_number') else None,
                        "account_number": "Y76383"
                    },
                    "reference2": order_data['order_number']
                }
            else:
                # Case B: Partial / Suffix Match
                if len(lookup_id.strip()) < 5:
                    conn.close()
                    return None, f"Input '{lookup_id}' is too short for a partial match. Please enter at least 5 characters or the full order number."

                # First try Suffix on Job Ticket
                cur.execute("""
                    SELECT j.id as job_id, j.order_id, j.job_ticket_number, o.order_number, 
                           o.ship_to_company, o.ship_to_name, 
                           o.address1, o.city, o.state, o.zip, o.country, o.store_number 
                    FROM jobs j
                    JOIN orders o ON j.order_id = o.id
                    WHERE j.job_ticket_number LIKE %s
                    ORDER BY j.id DESC LIMIT 1
                """, (f'%{lookup_id}',))
                job_data = cur.fetchone()

                if job_data:
                    # Instead of fetching all jobs for the order_id, only fetch the matched job
                    target_job_ids = [job_data['job_id']]
                    
                    response_data = {
                        "order_number": job_data['job_ticket_number'], 
                        "related_order_number": job_data['order_number'],
                        "searched_job_ticket": job_data['job_ticket_number'],
                        "ship_to": {
                            "name": job_data['ship_to_name'],
                            "company": job_data['ship_to_company'],
                            "address1": job_data['address1'],
                            "city": job_data['city'],
                            "state": job_data['state'],
                            "zip": job_data['zip'],
                            "country": job_data['country'],
                            "store_number": str(job_data['store_number']).strip().zfill(4) if job_data.get('store_number') else None,
                            "account_number": "Y76383"
                        },
                        "reference2": job_data['job_ticket_number'],
                        "match_type": "partial"
                    }
                else:
                    # Next try Suffix on Order Number
                    cur.execute("""
                        SELECT id, order_number, ship_to_company, ship_to_name, 
                               address1, city, state, zip, country, store_number 
                        FROM orders 
                        WHERE order_number LIKE %s
                        ORDER BY id DESC LIMIT 1
                    """, (f'%{lookup_id}',))
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
                            "store_number": str(order_data['store_number']).strip().zfill(4) if order_data.get('store_number') else None,
                            "account_number": "Y76383"
                        },
                        "reference2": order_data['order_number'],
                        "match_type": "partial"
                    }

        # 3. Items
        if target_job_ids:
            # Fetch Rules for Weights
            cur.execute("""
                SELECT category_name, quantity, box_weight, 
                       white_box_weight, blue_box_weight, white_box_qty, blue_box_qty 
                FROM product_shipping_rules
            """)
            rules = {(r['category_name'], r['quantity']): r for r in cur.fetchall()}
            
            cur.execute("""
                SELECT b.barcode_value, b.status, b.packed_at, b.box_sequence, i.sku, i.sku_description, i.order_item_id, 
                       i.quantity_ordered, i.cost_center, i.product_id, i.job_ticket_display_id, j.job_ticket_number,
                       s.tracking_number, s.marcom_response_message
                FROM items i
                LEFT JOIN item_boxes b ON i.order_item_id = b.order_item_id
                LEFT JOIN shipments s ON b.shipment_uid = s.shipment_uid
                JOIN jobs j ON i.job_id = j.id
                WHERE i.job_id = ANY(%s)
                ORDER BY j.job_ticket_number, i.order_item_id, b.box_sequence NULLS FIRST
            """, (target_job_ids,))
            
            rows = cur.fetchall()
            
            all_barcodes = []
            seen_items = {}
            for row in rows:
                code = row['barcode_value']
                is_master_scan = False
                if not code:
                    code = row['order_item_id']
                    is_master_scan = True
                
                all_barcodes.append(code)
                oid = row['order_item_id']
                if oid not in seen_items:
                    seen_items[oid] = {
                        "job_ticket": row['job_ticket_number'],
                        "job_ticket_display_id": row['job_ticket_display_id'],
                        "sku": row['sku'],
                        "sku_description": row['sku_description'],
                        "quantity_ordered": row['quantity_ordered'],
                        "barcodes": []
                    }
                
                # Weight Calc
                cat = get_product_category(row['product_id'])
                q = row['quantity_ordered']
                seq = row['box_sequence'] or 1
                
                unknown_weight = False
                rule = rules.get((cat, q))
                if rule:
                    white_qty = rule['white_box_qty'] or 0
                    if seq <= white_qty and rule['white_box_weight'] is not None:
                        est_weight = rule['white_box_weight']
                    elif rule['blue_box_weight'] is not None:
                        est_weight = rule['blue_box_weight']
                    elif rule['box_weight'] is not None:
                        est_weight = rule['box_weight']
                    else:
                        est_weight = 1.0
                        unknown_weight = True
                else:
                    est_weight = 1.0
                    unknown_weight = True

                # Format Date
                packed_at_str = None
                if row['packed_at']:
                    # Format: YYYY-MM-DD (ISO 8601) for correct string comparison
                    packed_at_str = row['packed_at'].strftime("%Y-%m-%d")

                # Shipped logic
                is_shipped = False
                if row.get('tracking_number') and row.get('marcom_response_message'):
                    if f"Item {oid}: Code: 3" in row['marcom_response_message']:
                        is_shipped = True
                
                seen_items[oid]['barcodes'].append({
                    "value": code,
                    "status": row.get('status'),
                    "packed_at": packed_at_str,
                    "estimated_weight": float(est_weight),
                    "unknown_weight": unknown_weight,
                    "is_master_scan": is_master_scan,
                    "is_shipped": is_shipped,
                    "tracking_number": row.get('tracking_number')
                })
            
            response_data['expected_barcodes'] = all_barcodes
            response_data['line_items'] = list(seen_items.values())

            # Determine Order Status
            total_boxes = len(rows)
            packed_boxes = sum(1 for r in rows if r.get('status') == 'packed')
            shipped_boxes = sum(1 for row in rows if row.get('tracking_number') and row.get('marcom_response_message') and f"Item {row['order_item_id']}: Code: 3" in row['marcom_response_message'])

            if total_boxes > 0 and shipped_boxes == total_boxes:
                response_data['status'] = 'SHIPPED'
            elif shipped_boxes > 0:
                response_data['status'] = 'PARTIALLY SHIPPED'
            elif total_boxes > 0 and packed_boxes == total_boxes:
                response_data['status'] = 'PACKED'
            elif packed_boxes > 0:
                response_data['status'] = 'PARTIALLY PACKED'
            else:
                response_data['status'] = 'OPEN'

        conn.close()
        return response_data, None



    except Exception as e:
        if conn: conn.close()
        print(e)
        return None, str(e)

def search_orders(query_str):
    """
    Search for orders or jobs that contain the query string.
    Returns a list of suggestion strings.
    """
    conn = get_db_connection()
    if not conn: return [], "DB Connection Error"
    
    try:
        cur = get_real_dict_cursor(conn)
        
        # Limit results to 10 for autocomplete
        # Search both job ticket and order number
        cur.execute("""
            SELECT DISTINCT job_ticket_number as match_val 
            FROM jobs 
            WHERE job_ticket_number ILIKE %s
            UNION
            SELECT DISTINCT order_number as match_val
            FROM orders
            WHERE order_number ILIKE %s
            ORDER BY match_val ASC
            LIMIT 10
        """, (f'%{query_str}%', f'%{query_str}%'))
        
        rows = cur.fetchall()
        conn.close()
        
        return [r['match_val'] for r in rows], None
        
    except Exception as e:
        if conn: conn.close()
        return [], str(e)


def compare_addresses(current_address_obj, new_lookup_id):
    conn = get_db_connection()
    if not conn: return None, "DB Connection Error"
    
    try:
        cur = get_real_dict_cursor(conn)
        
        # Reuse get_job_details logic or just fetch the ship_to params
        # For efficiency, let's just fetch the minimal needed
        # Check Job Ticket first
        cur.execute("""
            SELECT j.job_ticket_number, o.order_number, 
                   o.ship_to_company, o.ship_to_name, 
                   o.address1, o.city, o.state, o.zip, o.country 
            FROM jobs j
            JOIN orders o ON j.order_id = o.id
            WHERE j.job_ticket_number = %s
        """, (new_lookup_id,))
        job_data = cur.fetchone()
        
        # If not job, check order
        if not job_data:
            cur.execute("""
                SELECT order_number, ship_to_company, ship_to_name, 
                       address1, city, state, zip, country 
                FROM orders 
                WHERE order_number = %s
            """, (new_lookup_id,))
            job_data = cur.fetchone()
            # If still nothing, try partial match if >= 5 chars
            if not job_data:
                if len(new_lookup_id.strip()) >= 5:
                    # First try Suffix on Job Ticket
                    cur.execute("""
                        SELECT j.job_ticket_number, o.order_number, 
                               o.ship_to_company, o.ship_to_name, 
                               o.address1, o.city, o.state, o.zip, o.country 
                        FROM jobs j
                        JOIN orders o ON j.order_id = o.id
                        WHERE j.job_ticket_number ILIKE %s
                        ORDER BY j.id DESC LIMIT 1
                    """, (f'%{new_lookup_id}',))
                    job_data = cur.fetchone()
                    
                    if not job_data:
                        # Next try Suffix on Order Number
                        cur.execute("""
                            SELECT order_number, ship_to_company, ship_to_name, 
                                   address1, city, state, zip, country 
                            FROM orders 
                            WHERE order_number ILIKE %s
                            ORDER BY id DESC LIMIT 1
                        """, (f'%{new_lookup_id}',))
                        job_data = cur.fetchone()

            # If still nothing, error
            if not job_data:
                conn.close()
                return None, f"ID {new_lookup_id} not found"
        
        # Normailize Address Data
        new_address = {
            "company": job_data.get('ship_to_company', ''),
            "name": job_data.get('ship_to_name', ''),
            "address1": job_data.get('address1', ''),
            "city": job_data.get('city', ''),
            "state": job_data.get('state', ''),
            "zip": job_data.get('zip', '')
        }
        
        # Comparison Logic
        # 1. Strict Store # Check
        curr_store = extract_store_number_strict(current_address_obj.get('company', '') + " " + current_address_obj.get('address1', ''))
        new_store = extract_store_number_strict(new_address.get('company', '') + " " + new_address.get('address1', ''))
        
        status = 'mismatch'
        
        if curr_store and new_store and curr_store == new_store:
            status = 'exact_match'
        else:
            # 2. Fuzzy Address String Check
            # Create comparable strings
            s1 = f"{current_address_obj.get('address1','')} {current_address_obj.get('zip','')} {current_address_obj.get('company','')}"
            s2 = f"{new_address['address1']} {new_address['zip']} {new_address['company']}"
            
            ratio = fuzz.ratio(s1.lower(), s2.lower())
            if ratio > 90: status = 'exact_match'
            elif ratio > 75: status = 'fuzzy_match'
        
        conn.close()
        
        new_order_data = {}
        if status in ['exact_match', 'fuzzy_match']:
            # If it's a match, we need the FULL order details including items so the frontend can merge it
            full_details, err = get_job_details(new_lookup_id)
            if not err:
                new_order_data = full_details
        else:
            new_order_data = {
                "order_number": job_data.get('job_ticket_number') or job_data.get('order_number'),
                "ship_to": new_address
            }
        
        return {
            "status": status,
            "new_order_data": new_order_data
        }, None

    except Exception as e:
        if conn: conn.close()
        return None, str(e)
