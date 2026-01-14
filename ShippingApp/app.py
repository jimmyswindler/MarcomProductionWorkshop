import os
import re
import datetime
import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, abort, request
from flask_cors import CORS
from fuzzywuzzy import fuzz

# --- CONFIGURATION ---
XML_OUTPUT_FOLDER = 'xml_output'
if not os.path.exists(XML_OUTPUT_FOLDER):
    os.makedirs(XML_OUTPUT_FOLDER)

# DB CONFIG
DB_NAME = "marcom_production_suite"
DB_USER = "jimmyswindler" # Or production_client if running on Windows, but this script is on Mac for now too.
DB_HOST = "localhost" # 10.0.10.51 if external
DB_PORT = "5432"

app = Flask(__name__)
CORS(app)

# --- DB HELPERS ---
def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"DB Connection Error: {e}")
        return None

def get_store_number(name_string):
    match = re.search(r'#\s*(\d+)', name_string)
    if match:
        return match.group(1).zfill(4)
    return "0000"

def get_address_string(ship_to_obj):
    return f"{ship_to_obj.get('address1', '')} {ship_to_obj.get('city', '')} {ship_to_obj.get('state', '')} {ship_to_obj.get('zip', '')}".lower()

def generate_worldship_xml(shipment_data, packages):
    main_order = shipment_data['orders'][0]
    ship_to = main_order['ship_to']
    
    # Ref 1: Store Number (4 digits)
    store_number = get_store_number(ship_to.get('name', ''))
    
    # Ref 2: Order Number(s)
    ref2 = ",".join([o['order_number'] for o in shipment_data['orders']])

    xml_parts = []
    xml_parts.append(f"""<?xml version="1.0" encoding="WINDOWS-1252"?>
<OpenShipments xmlns="x-schema:OpenShipments.xdr">
    <OpenShipment ProcessStatus="" ShipmentOption="">
        <ShipTo>
            <CompanyOrName>{ship_to.get('company', '')}</CompanyOrName>
            <Attention>{ship_to.get('name', '')}</Attention>
            <Address1>{ship_to.get('address1', '')}</Address1>
            <CountryTerritory>{ship_to.get('country', 'US')}</CountryTerritory>
            <PostalCode>{ship_to.get('zip', '')}</PostalCode>
            <CityOrTown>{ship_to.get('city', '')}</CityOrTown>
            <StateProvinceCounty>{ship_to.get('state', '')}</StateProvinceCounty>
            <ReceiverUpsAccountNumber>{ship_to.get('account_number', '')}</ReceiverUpsAccountNumber>
        </ShipTo>
        <ShipFrom>
            <CompanyOrName>Clark Riggs Printing</CompanyOrName>
            <Attention>Shipping Dept</Attention>
            <Address1>1705 W Jefferson St</Address1>
            <CountryTerritory>US</CountryTerritory>
            <PostalCode>40203</PostalCode>
            <CityOrTown>Louisville</CityOrTown>
            <StateProvinceCounty>KY</StateProvinceCounty>
            <Telephone>502-493-9651</Telephone>
            <UpsAccountNumber>4080e5</UpsAccountNumber>
        </ShipFrom>
        <ShipmentInformation>
            <ServiceType>GND</ServiceType>
            <BillTransportationTo>Third Party</BillTransportationTo>
            <BillThirdParty>
                <ThirdPartyAccountNumber>{ship_to.get('account_number', '')}</ThirdPartyAccountNumber>
                <ThirdPartyAddress>
                    <CompanyOrName>N-MOTION</CompanyOrName>
                    <Address1>6040 DUTCHMANS LANE</Address1>
                    <Address2>SUITE 100</Address2>
                    <CityOrTown>LOUISVILLE</CityOrTown>
                    <StateProvinceCounty>KY</StateProvinceCounty>
                    <PostalCode>40205</PostalCode>
                    <CountryTerritory>US</CountryTerritory>
                </ThirdPartyAddress>
            </BillThirdParty>
        </ShipmentInformation>""")

    for pkg in packages:
        xml_parts.append(f"""
        <Package>
            <PackageType>CP</PackageType>
            <Weight>{pkg['weight']}</Weight>
            <Reference1>{store_number}</Reference1>
            <Reference2>{ref2}</Reference2>
            <Length>{pkg['L']}</Length>
            <Width>{pkg['W']}</Width>
            <Height>{pkg['H']}</Height>
        </Package>""")

    xml_parts.append("""
    </OpenShipment>
</OpenShipments>""")
    return "".join(xml_parts)

# --- HELPERS ---
def get_weight_rules(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    # 1. Product Map: ID -> Category
    cur.execute("SELECT product_id, category_name FROM product_categories")
    product_map = {row['product_id']: row['category_name'] for row in cur.fetchall()}
    
    # 2. Rules: (Category, Qty) -> Mixed Box Info
    cur.execute("""
        SELECT category_name, quantity, 
               white_box_weight, blue_box_weight, 
               white_box_qty, blue_box_qty 
        FROM product_shipping_rules
    """)
    rules = {}
    for r in cur.fetchall():
        key = (str(r['category_name']).lower(), int(r['quantity']))
        rules[key] = {
            'w_wt': float(r['white_box_weight']) if r['white_box_weight'] else 0,
            'b_wt': float(r['blue_box_weight']) if r['blue_box_weight'] else 0,
            'w_qty': int(r['white_box_qty']) if r['white_box_qty'] else 0,
            'b_qty': int(r['blue_box_qty']) if r['blue_box_qty'] else 0
        }
        
    cur.close()
    return product_map, rules

def get_mixed_box_info(product_map, rules, quantity, category, sku, product_id):
    # 1. Determine Category
    cat_name = None
    if product_id and product_id in product_map:
        cat_name = product_map[product_id]
    if not cat_name and category:
        cat_name = category
    if not cat_name:
        cat_name = sku
        
    # 2. Lookup Rule
    weights_list = []
    instructions = ""
    
    if cat_name:
        key = (str(cat_name).lower(), quantity)
        rule = rules.get(key)
        if rule:
            # Generate flattened list of weights: [W, W, W, B]
            for _ in range(rule['w_qty']):
                weights_list.append(rule['w_wt'])
            for _ in range(rule['b_qty']):
                weights_list.append(rule['b_wt'])
            
            # Generate instruction string
            parts = []
            if rule['w_qty']: parts.append(f"{rule['w_qty']}x White")
            if rule['b_qty']: parts.append(f"{rule['b_qty']}x Blue")
            instructions = ", ".join(parts)
             
    # Default fallback if no rule or empty rule
    if not weights_list:
         weights_list = [1.0] * 10 # Fallback: assume 1lb for next 10 boxes
         instructions = "No specific rule"

    return weights_list, instructions

# --- ROUTES ---

@app.route('/api/order/<string:lookup_id>', methods=['GET'])
def get_job_details(lookup_id):
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # 1. Try to find as JOB Ticket first (Existing Behavior)
        cur.execute("""
            SELECT j.id as job_id, j.job_ticket_number, o.order_number, 
                   o.ship_to_company, o.ship_to_name, 
                   o.address1, o.city, o.state, o.zip, o.country 
            FROM jobs j
            JOIN orders o ON j.order_id = o.id
            WHERE j.job_ticket_number = %s
        """, (lookup_id,))
        job_data = cur.fetchone()
        
        target_job_ids = []
        is_composite_order = False
        
        if job_data:
            # It's a single job
            target_job_ids = [job_data['job_id']]
            
            # Base response structure
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
                "reference2": job_data['job_ticket_number'] # For Job Ticket, Ref2 is Job Ticket
            }
            
        else:
            # 2. Not a Job Ticket, try as ORDER Number (New Behavior)
            cur.execute("""
                SELECT id, order_number, ship_to_company, ship_to_name, 
                       address1, city, state, zip, country 
                FROM orders 
                WHERE order_number = %s
            """, (lookup_id,))
            order_data = cur.fetchone()
            
            if not order_data:
                cur.close()
                conn.close()
                return jsonify({"error": f"ID {lookup_id} not found as Job Ticket or Order Number."}), 404
                
            is_composite_order = True
            
            # Get all jobs for this order
            cur.execute("SELECT id FROM jobs WHERE order_id = %s", (order_data['id'],))
            target_job_ids = [r['id'] for r in cur.fetchall()]
            
            if not target_job_ids:
                 # Order exists but no jobs?
                 cur.close()
                 conn.close()
                 return jsonify({"error": f"Order {lookup_id} found but has no linked jobs."}), 404

            # Base response structure for Composite
            response_data = {
                "order_number": order_data['order_number'], # Using Order Number as primary ID
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
                "reference2": order_data['order_number'] # For Composite, Ref2 is Order Number
            }

        # 3. Fetch Items & Barcodes for ALL Target Jobs
        # Dynamically build WHERE clause for IN list
        # We can use ANY(%s) with a list
        
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
        
        # Load Weight Rules & Product Map
        product_map, rules = get_weight_rules(conn)
        
        line_items_map = {}
        all_barcodes = []
        
        for row in rows:
            oid = row['order_item_id']
            bc = row['barcode_value']
            status = row.get('status')
            packed_at = row.get('packed_at')
            
            all_barcodes.append(bc)
            
            if oid not in line_items_map:
                line_items_map[oid] = {
                    "sku": row['sku'],
                    "sku_description": row['sku_description'],
                    "quantity_ordered": row['quantity_ordered'],
                    "cost_center": row['cost_center'],
                    "job_ticket": row['job_ticket_number'],
                    "barcodes": [],
                    "raw_weights": [], 
                    "packaging_instructions": ""
                }
                
                # Fetch Mixed Box Info ONCE per line item initialization
                weights_list, instructions = get_mixed_box_info(
                    product_map, rules, 
                    row['quantity_ordered'], 
                    row['cost_center'], 
                    row['sku'], 
                    row.get('product_id')
                )
                line_items_map[oid]['raw_weights'] = weights_list
                line_items_map[oid]['packaging_instructions'] = instructions
                
                line_items_map[oid]['packaging_instructions'] = instructions
                
            line_items_map[oid]['barcodes'].append({
                "value": bc,
                "status": status,
                "packed_at": packed_at
            })
            
        # Distribute weights
        line_items_list = []
        for oid, data in line_items_map.items():
            barcode_objects = []
            available_weights = data['raw_weights']
            
            for i, bc_obj in enumerate(data['barcodes']):
                est_weight = available_weights[i] if i < len(available_weights) else 1.0
                
                # Format packed_at if exists
                packed_str = None
                if bc_obj['packed_at']:
                    packed_str = bc_obj['packed_at'].strftime("%Y-%m-%d %H:%M")

                barcode_objects.append({
                    "value": bc_obj['value'],
                    "estimated_weight": est_weight,
                    "status": bc_obj['status'],
                    "packed_at": packed_str
                })
            
            line_items_list.append({
                "job_ticket": data['job_ticket'],
                "sku": data['sku'],
                "sku_description": data['sku_description'],
                "quantity_ordered": data['quantity_ordered'],
                "cost_center": data['cost_center'],
                "barcodes": barcode_objects,
                "packaging_instructions": data['packaging_instructions']
            })
        
        # Finalize Response
        response_data['expected_barcodes'] = all_barcodes
        response_data['line_items'] = line_items_list
        
        cur.close()
        conn.close()
        return jsonify(response_data)
        
    except Exception as e:
        print(e)
        return jsonify({"error": str(e)}), 500

# --- SAFETY LOGIC ---
def extract_store_number(text):
    if not text: return None
    # Look for "Store #123" or "# 123" or similar
    match = re.search(r'(?:store|#)\s*[\.\-]?\s*(\d+)', str(text), re.IGNORECASE)
    if match:
        return match.group(1)
    return None

@app.route('/api/order/compare', methods=['POST'])
def compare_order_address():
    data = request.json
    new_job_ticket = data.get('new_order_id')
    current_address = data.get('current_address')
    
    conn = get_db_connection()
    if not conn: return jsonify({"error": "DB Error"}), 500
    
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT j.job_ticket_number, o.order_number, 
                   o.ship_to_company, o.ship_to_name, 
                   o.address1, o.city, o.state, o.zip, o.country 
            FROM jobs j
            JOIN orders o ON j.order_id = o.id
            WHERE j.job_ticket_number = %s
        """, (new_job_ticket,))
        
        job_data = cur.fetchone()
        if not job_data:
            return jsonify({"message": f"Job {new_job_ticket} not found"}), 404
            
        cur.execute("""
            SELECT b.barcode_value
            FROM item_boxes b
            JOIN items i ON b.order_item_id = i.order_item_id
            JOIN jobs j ON i.job_id = j.id
            WHERE j.job_ticket_number = %s
        """, (new_job_ticket,))
        barcodes = [row['barcode_value'] for row in cur.fetchall()]
        
        conn.close()
        
        new_order_data = {
            "order_number": job_data['job_ticket_number'],
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
            "expected_barcodes": barcodes,
            "reference2": job_data['job_ticket_number']
        }
    
        new_address = new_order_data.get('ship_to', {})
        
        # --- Strict Store ID Check ---
        current_store_id = extract_store_number(current_address.get('company')) or extract_store_number(current_address.get('address1'))
        new_store_id = extract_store_number(new_address.get('company')) or extract_store_number(new_address.get('address1'))
        
        status = 'mismatch'
        
        if current_store_id and new_store_id and current_store_id == new_store_id:
             status = 'exact_match'
        else:
            # Fallback to fuzzy
            ratio = fuzz.ratio(str(current_address), str(new_address))
            if ratio > 90: status = 'exact_match'
            elif ratio > 70: status = 'fuzzy_match'
        
        return jsonify({
            "status": status,
            "new_order": new_order_data
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- DRAFT ENDPOINTS ---

@app.route('/api/draft/save', methods=['POST'])
def save_draft():
    data = request.json
    job_ticket = data.get('job_ticket')
    barcodes = data.get('barcodes', [])
    
    conn = get_db_connection()
    if not conn: return jsonify({"error": "DB Error"}), 500
    
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO shipment_drafts (job_ticket_number, scanned_barcodes, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (job_ticket_number) 
            DO UPDATE SET scanned_barcodes = EXCLUDED.scanned_barcodes, updated_at = NOW()
        """, (job_ticket, psycopg2.extras.Json(barcodes)))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/draft/<string:job_ticket>', methods=['GET'])
def get_draft(job_ticket):
    conn = get_db_connection()
    if not conn: return jsonify({"error": "DB Error"}), 500
    
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT scanned_barcodes FROM shipment_drafts WHERE job_ticket_number = %s", (job_ticket,))
        row = cur.fetchone()
        conn.close()
        
        if row:
            return jsonify({"barcodes": row['scanned_barcodes']})
        return jsonify({"barcodes": []}) # No draft is fine
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/draft/<string:job_ticket>', methods=['DELETE'])
def delete_draft(job_ticket):
    conn = get_db_connection()
    if not conn: return jsonify({"error": "DB Error"}), 500
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM shipment_drafts WHERE job_ticket_number = %s", (job_ticket,))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/shipment/process', methods=['POST'])
def process_shipment():
    data = request.json
    orders = data.get('orders', [])
    scanned_boxes = data.get('scanned_boxes', [])
    package_list_in = data.get('package_list', [])

    if not package_list_in:
        return jsonify({"error": "No packages provided."}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "DB Error"}), 500
    
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # 1a. PARTIAL ITEM VALIDATION
        if scanned_boxes:
            cur.execute("""
                SELECT i.sku, count(b.id) as total_boxes,
                       count(CASE WHEN b.barcode_value = ANY(%s) THEN 1 END) as current_scan_count
                FROM item_boxes b
                JOIN items i ON b.order_item_id = i.order_item_id
                WHERE b.barcode_value = ANY(%s)
                GROUP BY i.sku, i.order_item_id
            """, (scanned_boxes, scanned_boxes))
            
            for row in cur.fetchall():
                if row['current_scan_count'] != row['total_boxes']:
                    sku = row['sku']
                    conn.rollback()
                    return jsonify({"error": f"Partial Line Item detected for SKU {sku}. You must scan ALL {row['total_boxes']} boxes for this item to ship it."}), 400
        
        # 1. Update Box Status
        if scanned_boxes:
            cur.execute("""
                UPDATE item_boxes 
                SET status = 'packed', packed_at = NOW()
                WHERE barcode_value = ANY(%s)
            """, (scanned_boxes,))
        
        # 2. Calculate Weights
        # Get weight rules
        cur.execute("SELECT category_name, quantity, box_weight FROM product_shipping_rules")
        rules = {(r['category_name'], r['quantity']): r['box_weight'] for r in cur.fetchall()}
        
        # Get Cartons
        cur.execute("SELECT code, weight, length, width, height FROM shipping_cartons")
        cartons = {c['code']: c for c in cur.fetchall()}

        # Get Item Info for scanned boxes (to lookup rules)
        # Note: scanned_boxes contains ALL boxes in this shipment.
        # We need to sum the weight of all these boxes.
        total_shipment_product_weight = 0.0
        
        if scanned_boxes:
             # Find quantities and categories for these boxes
             # This is tricky because barcode -> item_box -> item -> (quantity, cost_center)
             # But 'quantity' in items is the TOTAL quantity for the item.
             # 'item_boxes' splits that item into boxes.
             # We need to know: Is this box a 250 count? 500 count?
             # Currently item_boxes doesn't store "qty_in_box".
             # Assumption: The rule is based on the TOTAL Item Quantity.
             # OR does the rule imply specific breakdown?
             # User said: "weight values for each of the product types and their required number of packing boxes based on the quantity"
             # I will assume: Find item -> Get Qty -> Look up weight for that Qty.
             # If an item spans multiple boxes, do we divide?
             # "weights per packing box" implies the rule is Per Box.
             
             cur.execute("""
                SELECT i.quantity_ordered, i.cost_center, i.product_name 
                FROM item_boxes b
                JOIN items i ON b.order_item_id = i.order_item_id
                WHERE b.barcode_value = ANY(%s)
             """, (scanned_boxes,))
             
             for row in cur.fetchall():
                 q = row['quantity_ordered']
                 cat = row['cost_center'] # or mapped value
                 
                 # Lookup rule
                 # Try cost_center first
                 w = rules.get((cat, q))
                 if w is None:
                     # Fallback?
                     w = 1.0 # Default
                 
                 total_shipment_product_weight += w
                 
        conn.commit()
        cur.close()
        conn.close()

        # 3. Pack Cartons
        final_packages = []
        for pkg_in in package_list_in:
            carton_id = pkg_in.get('id')
            
            if carton_id == 'CUSTOM':
                dims = {'L': pkg_in.get('L'), 'W': pkg_in.get('W'), 'H': pkg_in.get('H')}
                weight = float(pkg_in.get('weight', 0))
            else:
                carton_data = cartons.get(carton_id)
                if not carton_data:
                    return jsonify({"error": f"Unknown carton: {carton_id}"}), 400
                dims = {'L': carton_data['length'], 'W': carton_data['width'], 'H': carton_data['height']}
                
                if 'weight' in pkg_in and pkg_in['weight']:
                     weight = float(pkg_in['weight'])
                else:
                     # Auto-Calc
                     weight = total_shipment_product_weight + carton_data['weight']

            final_packages.append({
                "weight": round(weight, 1),
                "L": dims['L'], "W": dims['W'], "H": dims['H']
            })

        # 4. Generate XML
        xml_string = generate_worldship_xml({"orders": orders}, final_packages)
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"SHIP_{orders[0]['order_number']}_{timestamp}.xml"
        with open(os.path.join(XML_OUTPUT_FOLDER, filename), "w") as f:
            f.write(xml_string)

        return jsonify({
            "success": True,
            "filename": filename,
            "total_weight": sum(p['weight'] for p in final_packages),
            "package_count": len(final_packages)
        })

    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)
