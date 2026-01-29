import os
import re
import datetime
import math
import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, abort, request, send_from_directory
from flask_cors import CORS
from fuzzywuzzy import fuzz
import requests
import xml.etree.ElementTree as ET

# --- CONFIGURATION ---
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

XML_OUTPUT_FOLDER = 'xml_output'
if not os.path.exists(XML_OUTPUT_FOLDER):
    os.makedirs(XML_OUTPUT_FOLDER)

# DB CONFIG
DB_NAME = os.getenv("DB_NAME", "marcom_production_suite")
DB_USER = os.getenv("DB_USER", "jimmyswindler")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

# Marcom Central Service Endpoints
MC_URL = os.getenv("MARCOM_URL", "https://services.printable.com/trans/1.0/PackingSlip.asmx")
SOAP_ACTION = os.getenv("MARCOM_SOAP_ACTION", "http://www.printable.com/WebService/PackingSlip/CreatePackingSlipByLineItem")
# !!! CRITICAL: Live Partner Token
PARTNER_TOKEN = os.getenv("MARCOM_PARTNER_TOKEN")
if not PARTNER_TOKEN:
    print("WARNING: MARCOM_PARTNER_TOKEN not found in .env")
DEFAULT_CARRIER = 'UPS'

# --- SIMULATION SETTINGS ---
# Set to TRUE to divert XML to a local folder instead of sending to Marcom.
MARCOM_SIMULATION_MODE = True
MARCOM_DEBUG_FOLDER = os.path.join(XML_OUTPUT_FOLDER, 'marcom_debug')
if MARCOM_SIMULATION_MODE and not os.path.exists(MARCOM_DEBUG_FOLDER):
    os.makedirs(MARCOM_DEBUG_FOLDER)

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

def generate_worldship_xml(shipment_data, packages, store_number_arg=None):
    main_order = shipment_data['orders'][0]
    ship_to = main_order['ship_to']
    
    # Ref 1: Store Number (4 digits)
    store_number_str = "0000"
    if store_number_arg:
         store_number_str = str(store_number_arg).strip().zfill(4)
    else:
         store_number_str = get_store_number(ship_to.get('name', ''))
    
    # Store Logic: 0001-1000 Override
    try:
        store_int = int(store_number_str)
    except ValueError:
        store_int = 99999 # Fallback if not numeric

    if 1 <= store_int <= 1000:
        final_company = f"Texas Roadhouse #{store_number_str.lstrip('0')}" # Remove leading zeros for display e.g. #244
        # Wait, user said "#[Store Number]" e.g. "#244". zfill(4) gives "0244".
        # Let's trust int conversion logic or lstrip?
        # User example: "#244". So remove leading 0s.
        final_attention = "Store Manager"
    else:
        final_company = ship_to.get('company', '')
        final_attention = ship_to.get('name', '')
    
    # Ref 2: Order Number(s)
    ref2 = ",".join([o['order_number'] for o in shipment_data['orders']])

    xml_parts = []
    xml_parts.append(f"""<?xml version="1.0" encoding="WINDOWS-1252"?>
<OpenShipments xmlns="x-schema:OpenShipments.xdr">
    <OpenShipment ProcessStatus="Y">
        <ShipTo>
            <CompanyOrName>{final_company}</CompanyOrName>
            <Attention>{final_attention}</Attention>
            <Address1>{ship_to.get('address1', '')}</Address1>
            <CountryTerritory>{ship_to.get('country', 'US')}</CountryTerritory>
            <PostalCode>{ship_to.get('zip', '')}</PostalCode>
            <CityOrTown>{ship_to.get('city', '')}</CityOrTown>
            <StateProvinceCounty>{ship_to.get('state', '')}</StateProvinceCounty>
            <ReceiverUpsAccountNumber>{ship_to.get('account_number', 'Y76383')}</ReceiverUpsAccountNumber>
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
        <ThirdParty>
            <CompanyOrName>N-MOTION</CompanyOrName>
            <Attention>Marney Bruner</Attention>
            <Address1>6040 Dutchman's Lane</Address1>
            <Address2>Suite 100</Address2>
            <CityOrTown>Louisville</CityOrTown>
            <CountryTerritory>US</CountryTerritory>
            <PostalCode>40205</PostalCode>
            <StateProvinceCounty>KY</StateProvinceCounty>
            <UpsAccountNumber>Y76383</UpsAccountNumber>
        </ThirdParty>
        <ShipmentInformation>
            <ServiceType>GND</ServiceType>
            <NumberOfPackages>{len(packages)}</NumberOfPackages>
            <BillTransportationTo>Third Party</BillTransportationTo>
        </ShipmentInformation>""")

    for pkg in packages:
        # Weight Rounding UP to nearest integer
        weight_raw = float(pkg.get('weight', 1.0))
        weight_int = int(math.ceil(weight_raw))
        
        # Dim conversion to int
        l_int = int(float(pkg.get('L', 0)))
        w_int = int(float(pkg.get('W', 0)))
        h_int = int(float(pkg.get('H', 0)))

        xml_parts.append(f"""
        <Package>
            <PackageType>CP</PackageType>
            <Weight>{weight_int}</Weight>
            <Reference1>{store_number_str}</Reference1>
            <Reference2>{ref2}</Reference2>
            <Length>{l_int}</Length>
            <Width>{w_int}</Width>
            <Height>{h_int}</Height>
            <MerchandiseDescription>PRINTED MATERIAL</MerchandiseDescription>
        </Package>""")

    xml_parts.append("""
    </OpenShipment>
</OpenShipments>""")
    return "".join(xml_parts)

def generate_marcom_xml_payload(line_item_id, tracking_number, token, carrier):
    return f"""
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:pac="http://www.printable.com/WebService/PackingSlip">
    <soapenv:Header/>
    <soapenv:Body>
        <pac:CreatePackingSlipByLineItem>
            <pac:pRequest>
                <PartnerCredentials>
                    <Token>{token}</Token>
                </PartnerCredentials>
                <PackingSlipNode>
                    <CarrierName>{carrier}</CarrierName>
                    <TrackingNumber>{tracking_number}</TrackingNumber>
                </PackingSlipNode>
                <LineItems>
                    <LineItem>
                        <ID type="Printable">{line_item_id}</ID>
                    </LineItem>
                </LineItems>
            </pac:pRequest>
        </pac:CreatePackingSlipByLineItem>
    </soapenv:Body>
</soapenv:Envelope>
"""

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
            SELECT j.id as job_id, j.order_id, j.job_ticket_number, o.order_number, 
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
        
        # 4. Calculate Global Order Progress
        parent_order_id = job_data['order_id'] if job_data else order_data['id']
        cur.execute("""
            SELECT count(b.id) as total,
                   count(CASE WHEN b.status = 'packed' THEN 1 END) as packed
            FROM item_boxes b
            JOIN items i ON b.order_item_id = i.order_item_id
            JOIN jobs j ON i.job_id = j.id
            WHERE j.order_id = %s
        """, (parent_order_id,))
        prog_row = cur.fetchone()
        
        response_data['order_progress'] = {
            "total_boxes": prog_row['total'] if prog_row else 0,
            "packed_boxes": prog_row['packed'] if prog_row else 0
        }
        
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
        
        store_number = None

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
                 
                 # Capture Store Number (cost_center) from first row if not set
                 if not store_number and row['cost_center']:
                     store_number = row['cost_center']
                 
        # Fallback: If store number not found in scanned boxes (e.g. manual bypass?)
        # Fetch from DB using Order/Job info
        if not store_number:
             try:
                 # Check if we can get it from the Order object passed in? NO, that's just UI data.
                 # Use the Order Number from input
                 main_ord_num = orders[0]['order_number']
                 cur.execute("""
                    SELECT i.cost_center 
                    FROM items i
                    JOIN jobs j ON i.job_id = j.id
                    WHERE j.job_ticket_number = %s
                    LIMIT 1
                 """, (main_ord_num,))
                 row = cur.fetchone()
                 if row:
                     store_number = row['cost_center']
                 else:
                     # Try as Order Number
                     cur.execute("""
                        SELECT i.cost_center 
                        FROM items i
                        JOIN jobs j ON i.job_id = j.id
                        JOIN orders o ON j.order_id = o.id
                        WHERE o.order_number = %s
                        LIMIT 1
                     """, (main_ord_num,))
                     row = cur.fetchone()
                     if row: store_number = row['cost_center']
             except Exception as e:
                 print(f"Error fetching fallback store number: {e}")
                 
                
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

        # 4. Generate ShipmentID & DB Record
        import random
        timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        rand_suffix = str(random.randint(1000, 9999))
        shipment_uid = f"SHIP_{timestamp_str}_{rand_suffix}"
        
        # Gather packed item details for summary BEFORE closing connection
        packed_summary = []
        if scanned_boxes:
             cur.execute("""
                SELECT j.job_ticket_number, i.sku, i.product_name, count(b.id) as box_count
                FROM item_boxes b
                JOIN items i ON b.order_item_id = i.order_item_id
                JOIN jobs j ON i.job_id = j.id
                WHERE b.barcode_value = ANY(%s)
                GROUP BY j.job_ticket_number, i.sku, i.product_name
                ORDER BY j.job_ticket_number, i.sku
             """, (scanned_boxes,))
             for row in cur.fetchall():
                 packed_summary.append(f"Job {row['job_ticket_number']}: {row['sku']} ({row['box_count']} boxes)")

        ref_order_number = orders[0]['order_number'] if orders else None
        
        cur.execute("""
            INSERT INTO shipments (shipment_uid, job_ticket_number, marcom_sync_status, created_at)
            VALUES (%s, %s, 'PROCESSING', NOW())
            RETURNING id
        """, (shipment_uid, ref_order_number))
        
        conn.commit()
        cur.close()
        conn.close()
        
        # 5. Generate XML
        xml_string = generate_worldship_xml({"orders": orders}, final_packages, store_number)
        
        filename = f"{shipment_uid}.xml"
        with open(os.path.join(XML_OUTPUT_FOLDER, filename), "w") as f:
            f.write(xml_string)

        return jsonify({
            "success": True,
            "filename": filename,
            "shipment_uid": shipment_uid,
            "total_weight": sum(p['weight'] for p in final_packages),
            "package_count": len(final_packages),
            "ship_to": orders[0]['ship_to'] if orders else {},
            "packed_items": packed_summary
        })

    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error": str(e)}), 500

# --- MARCOM SYNC ROUTES ---

@app.route('/api/activity_feed', methods=['GET'])
def get_activity_feed():
    conn = get_db_connection()
    if not conn: return jsonify({"error": "DB Error"}), 500
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # Fetch last 50 shipments
        cur.execute("""
            SELECT job_ticket_number, tracking_number, marcom_sync_status,
                   marcom_response_message, reference_id, created_at
            FROM shipments
            ORDER BY created_at DESC
            LIMIT 50
        """)
        rows = cur.fetchall()

        # Format timestamps
        feed = []
        for r in rows:
            r['created_at'] = r['created_at'].strftime("%H:%M:%S")
            feed.append(r)

        return jsonify(feed)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/shipment/close', methods=['POST'])
def close_shipment():
    data = request.json
    tracking_number = data.get('tracking_number')
    # We need to know WHICH order/job this is for.
    # In the UI, the user is focusing on an Order ID (which is either JobTicket or OrderNum).
    # Ideally, we pass that ID too.
    job_ticket_or_order = data.get('order_id') 
    
    if not tracking_number or not job_ticket_or_order:
        return jsonify({"error": "Missing tracking number or order ID"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "DB Error"}), 500

    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # 1. Identify Line Items to Close
        # We need the 'order_item_id' which maps to Marcom's Line Item ID?
        # Re-using logic from get_job_details to find the jobs
        
        # Check if it's a Job Ticket
        cur.execute("SELECT id FROM jobs WHERE job_ticket_number = %s", (job_ticket_or_order,))
        job_row = cur.fetchone()
        
        target_job_ids = []
        if job_row:
             target_job_ids = [job_row['id']]
        else:
             # Assume Order Number
             cur.execute("SELECT id FROM orders WHERE order_number = %s", (job_ticket_or_order,))
             order_row = cur.fetchone()
             if order_row:
                 cur.execute("SELECT id FROM jobs WHERE order_id = %s", (order_row['id'],))
                 target_job_ids = [r['id'] for r in cur.fetchall()]
        
        if not target_job_ids:
             return jsonify({"error": "Order ID not found"}), 404

        # Get all relevant items for these jobs
        scanned_barcodes = data.get('scanned_barcodes')
        
        if scanned_barcodes and len(scanned_barcodes) > 0:
            # PARTIAL/SPECIFIC SHIPMENT:
            # Sync only the items associated with the scanned boxes.
            cur.execute("""
                SELECT DISTINCT i.order_item_id 
                FROM item_boxes b
                JOIN items i ON b.order_item_id = i.order_item_id
                WHERE b.barcode_value = ANY(%s)
            """, (scanned_barcodes,))
        else:
            # FALLBACK / FULL ORDER:
            # If no barcodes provided (e.g. legacy call), try to close ALL items in the order.
            cur.execute("""
                SELECT i.order_item_id 
                FROM items i 
                WHERE i.job_id = ANY(%s)
            """, (target_job_ids,))
        
        line_items = [r['order_item_id'] for r in cur.fetchall()]
        
        results = []
        
        for li_id in line_items:
            # 2. Call Marcom Central for EACH item
            soap_xml = generate_marcom_xml_payload(li_id, tracking_number, PARTNER_TOKEN, DEFAULT_CARRIER)
            
            # --- SIMULATION BRANCH ---
            if MARCOM_SIMULATION_MODE:
                # Mock Success
                status = "SUCCESS"
                msg_attr = "SIMULATED: Packing Slip Created"
                
                # Write to file for audit
                filename = f"MARCOM_SIM_{job_ticket_or_order}_{li_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xml"
                with open(os.path.join(MARCOM_DEBUG_FOLDER, filename), "w") as f:
                    f.write(soap_xml)
                    
                print(f"[SIMULATOR] Saved XML to {filename}")
                
            else:
                # --- LIVE BRANCH ---
                headers = {'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': SOAP_ACTION}
                try:
                    res = requests.post(MC_URL, data=soap_xml.encode('utf-8'), headers=headers, verify=True)
                    # Parse Response
                    if "ProcessComplete" in res.text:
                        status = "SUCCESS"
                        msg_attr = "Packing Slip Created"
                    else:
                        status = "FAILED"
                        msg_attr = "Marcom Error"
                except Exception as req_err:
                        status = "ERROR"
                        msg_attr = str(req_err)

            # Store Result in DB (Common for both)
            try:
                cur.execute("""
                    INSERT INTO shipments (job_ticket_number, tracking_number, marcom_sync_status, marcom_response_message, reference_id)
                    VALUES (%s, %s, %s, %s, %s)
                """, (job_ticket_or_order, tracking_number, status, msg_attr, str(li_id)))
            except Exception as db_err:
                 print(f"DB Error Log: {db_err}")
            
            results.append({"line_item": li_id, "status": status})

        conn.commit()
        return jsonify({"success": True, "results": results})

    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# --- FRONTEND SERVING ---
@app.route('/')
@app.route('/shipping_station_feed.html')
def serve_frontend():
    return send_from_directory('.', 'shipping_station_feed.html')

if __name__ == '__main__':
    app.run(debug=True, port=5001)
