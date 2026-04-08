import math
import os
import datetime
from shared_lib.database import get_db_connection, get_real_dict_cursor
from shared_lib.config import get_env_var
from shared_lib.utils import get_store_number, get_product_category
from xml.sax.saxutils import escape

def sanitize_xml(val):
    if not val:
        return ""
    s = str(val)
    s = s.replace('‘', "'").replace('’', "'").replace('“', '"').replace('”', '"')
    return escape(s)

def get_shipping_cartons():
    conn = get_db_connection()
    if not conn: return {}, "DB Connection Failed"
    
    try:
        cur = get_real_dict_cursor(conn)
        cur.execute("SELECT code, weight FROM shipping_cartons")
        cartons = {c['code']: float(c['weight']) for c in cur.fetchall() if c['weight'] is not None}
        conn.close()
        return cartons, None
    except Exception as e:
        if conn: conn.close()
        return {}, str(e)

def get_shipping_stations():
    conn = get_db_connection()
    if not conn: return [], "DB Connection Failed"
    
    try:
        cur = get_real_dict_cursor(conn)
        cur.execute("SELECT station_id, display_name FROM shipping_stations WHERE is_active = TRUE ORDER BY display_name")
        stations = cur.fetchall()
        conn.close()
        return stations, None
    except Exception as e:
        if conn: conn.close()
        return [], str(e)

def generate_worldship_xml(shipment_data, packages, store_number_arg=None):
    # ... copied logic ...
    main_order = shipment_data['orders'][0]
    ship_to = main_order['ship_to']
    
    store_number_str = "0000"
    if store_number_arg:
         store_number_str = str(store_number_arg).strip().zfill(4)
    else:
         store_number_str = get_store_number(ship_to.get('name', ''))
    
    try:
        store_int = int(store_number_str)
    except ValueError:
        store_int = 99999 

    original_company = ship_to.get('company')
    if not original_company or str(original_company).strip() == "":
        original_company = "Texas Roadhouse"
    else:
        original_company = str(original_company).strip()

    final_attention = ship_to.get('name')
    if not final_attention or str(final_attention).strip() == "":
        final_attention = "Store Manager"
    else:
        final_attention = str(final_attention).strip()

    formatted_store_num = store_number_str.lstrip('0')
    if formatted_store_num and formatted_store_num != "0":
        store_suffix = f" #{formatted_store_num}"
        if store_suffix not in original_company:
            final_company = f"{original_company}{store_suffix}"
        else:
            final_company = original_company
    else:
        final_company = original_company
    
    unique_numeric_orders = []
    for o in shipment_data['orders']:
        order_str = o.get('related_order_number') or o.get('order_number') or ""
        parts = str(order_str).split('-')
        num_part = parts[-1] if len(parts) > 1 else str(order_str)
        if num_part and num_part not in unique_numeric_orders:
            unique_numeric_orders.append(num_part)
            
    ref2 = ", ".join(unique_numeric_orders)

    xml_parts = []
    xml_parts.append(f"""<?xml version="1.0" encoding="UTF-8"?>
<OpenShipments xmlns="x-schema:OpenShipments.xdr">
    <OpenShipment ProcessStatus="Y">
        <ShipTo>
            <CompanyOrName>{sanitize_xml(final_company)}</CompanyOrName>
            <Attention>{sanitize_xml(final_attention)}</Attention>
            <Address1>{sanitize_xml(ship_to.get('address1', ''))}</Address1>
            <CountryTerritory>{sanitize_xml(ship_to.get('country', 'US'))}</CountryTerritory>
            <PostalCode>{sanitize_xml(ship_to.get('zip', ''))}</PostalCode>
            <CityOrTown>{sanitize_xml(ship_to.get('city', ''))}</CityOrTown>
            <StateProvinceCounty>{sanitize_xml(ship_to.get('state', ''))}</StateProvinceCounty>
            <ReceiverUpsAccountNumber>{sanitize_xml(ship_to.get('account_number', 'Y76383'))}</ReceiverUpsAccountNumber>
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
        weight_raw = float(pkg.get('weight', 1.0))
        weight_str = f"{weight_raw:.2f}"
        
        # Handle blank/None dimensions
        l_val = pkg.get('L')
        w_val = pkg.get('W')
        h_val = pkg.get('H')
        
        l_str = str(int(float(l_val))) if l_val is not None and l_val != '' else ""
        w_str = str(int(float(w_val))) if w_val is not None and w_val != '' else ""
        h_str = str(int(float(h_val))) if h_val is not None and h_val != '' else ""

        xml_parts.append(f"""
        <Package>
            <PackageType>CP</PackageType>
            <Weight>{weight_str}</Weight>
            <Reference1>{sanitize_xml(store_number_str)}</Reference1>
            <Reference2>{sanitize_xml(ref2)}</Reference2>
            <Length>{l_str}</Length>
            <Width>{w_str}</Width>
            <Height>{h_str}</Height>
            <MerchandiseDescription>PRINTED MATERIAL</MerchandiseDescription>
        </Package>""")

    xml_parts.append("""
    </OpenShipment>
</OpenShipments>""")
    return "".join(xml_parts)

def process_shipment_logic(orders, scanned_boxes, package_list_in, station_id=None):
    if not station_id or station_id == 'null':
        return {"error": "A Shipping Station must be selected."}, 400

    conn = get_db_connection()
    if not conn: raise Exception("DB Connection Failed")
    
    try:
        cur = get_real_dict_cursor(conn)
        
        # 0. JIT Box Insertion for Master Scans
        if scanned_boxes:
            updated_scanned_boxes = []
            for box in scanned_boxes:
                # If it's a base order_item_id (8 digits), insert the Z box
                if len(box) == 8 and box.isdigit():
                    z_barcode = f"{box}Z"
                    cur.execute("""
                        INSERT INTO item_boxes (order_item_id, box_sequence, barcode_value)
                        VALUES (%s, 1, %s)
                        ON CONFLICT (order_item_id, box_sequence) DO NOTHING
                    """, (box, z_barcode))
                    updated_scanned_boxes.append(z_barcode)
                else:
                    updated_scanned_boxes.append(box)
            
            scanned_boxes = updated_scanned_boxes

        # 1a. Partial Check
        if scanned_boxes:
            cur.execute("""
                WITH matched_jobs AS (
                    SELECT DISTINCT i.job_id
                    FROM item_boxes b
                    JOIN items i ON b.order_item_id = i.order_item_id
                    WHERE b.barcode_value = ANY(%s)
                )
                SELECT j.job_ticket_number, 
                       count(b.id) as total_boxes,
                       count(CASE WHEN b.status = 'packed' OR b.barcode_value = ANY(%s) THEN 1 END) as current_scan_count
                FROM jobs j
                JOIN items i ON j.id = i.job_id
                JOIN item_boxes b ON i.order_item_id = b.order_item_id
                WHERE j.id IN (SELECT job_id FROM matched_jobs)
                GROUP BY j.job_ticket_number
            """, (scanned_boxes, scanned_boxes))
            
            for row in cur.fetchall():
                if row['current_scan_count'] != row['total_boxes']:
                    conn.close()
                    return {"error": f"Partial Job detected for Job Ticket {row['job_ticket_number']}. Scan all boxes for this job."}, 400

        # 1. Update Box Status
        if scanned_boxes:
            cur.execute("""
                UPDATE item_boxes 
                SET status = 'packed', packed_at = NOW()
                WHERE barcode_value = ANY(%s)
            """, (scanned_boxes,))

        # 2. Calculate Weights (Simplified for now, similar to original)
        cur.execute("""
            SELECT category_name, quantity, box_weight, 
                   white_box_weight, blue_box_weight, white_box_qty, blue_box_qty 
            FROM product_shipping_rules
        """)
        rules = {(r['category_name'], r['quantity']): r for r in cur.fetchall()}
        
        cur.execute("SELECT code, weight, length, width, height FROM shipping_cartons")
        cartons = {c['code']: c for c in cur.fetchall()}
        
        total_shipment_product_weight = 0.0
        store_number = None
        missing_weight_error = None
        
        if scanned_boxes:
             cur.execute("""
                SELECT i.quantity_ordered, i.cost_center, i.product_id, b.box_sequence
                FROM item_boxes b
                JOIN items i ON b.order_item_id = i.order_item_id
                WHERE b.barcode_value = ANY(%s)
             """, (scanned_boxes,))
             for row in cur.fetchall():
                 cat = get_product_category(row['product_id'])
                 q = row['quantity_ordered']
                 seq = row['box_sequence'] or 1
                 
                 rule = rules.get((cat, q))
                 if rule:
                     white_qty = rule['white_box_qty'] or 0
                     if seq <= white_qty and rule['white_box_weight'] is not None:
                         w = rule['white_box_weight']
                     elif rule['blue_box_weight'] is not None:
                         w = rule['blue_box_weight']
                     elif rule['box_weight'] is not None:
                         w = rule['box_weight']
                     else:
                         w = 0.0
                         if not missing_weight_error:
                             missing_weight_error = f"Item with product category '{cat}' and quantity {q} has no defined shipping weight rule. Manual weight entry required."
                 else:
                     w = 0.0
                     if not missing_weight_error:
                         missing_weight_error = f"Item with product category '{cat}' and quantity {q} has no defined shipping weight rule. Manual weight entry required."
                     
                 total_shipment_product_weight += w
                 if not store_number and row['cost_center']:
                     store_number = row['cost_center']

        if not store_number and orders:
             # Logic to fetch from orders logic if needed... or just use first order
             pass # Kept simple for now

        # 3. Pack Cartons
        final_packages = []
        for pkg_in in package_list_in:
            carton_id = pkg_in.get('id')
            if carton_id == 'CUSTOM':
                weight = float(pkg_in.get('weight', 0))
                dims = {'L': pkg_in.get('L'), 'W': pkg_in.get('W'), 'H': pkg_in.get('H')}
            else:
                carton_data = cartons.get(carton_id)
                if not carton_data: return {"error": f"Unknown carton: {carton_id}"}, 400
                dims = {'L': carton_data['length'], 'W': carton_data['width'], 'H': carton_data['height']}
                if 'weight' in pkg_in and pkg_in['weight']:
                     weight = float(pkg_in['weight'])
                else:
                     if missing_weight_error:
                         return {"error": missing_weight_error}, 400
                     weight = total_shipment_product_weight + carton_data['weight']
            
            final_packages.append({"weight": round(weight, 2), **dims})

        # 4. Generate Shipment ID (YYYYMMDD_XXXX)
        now = datetime.datetime.now()
        date_str = now.strftime("%Y%m%d")
        
        # Lock table to prevent race conditions during ID generation
        cur.execute("LOCK TABLE shipments IN ACCESS EXCLUSIVE MODE")
        
        # Find the last ID for today to increment
        cur.execute("""
            SELECT shipment_uid 
            FROM shipments 
            WHERE shipment_uid LIKE %s
            ORDER BY shipment_uid DESC 
            LIMIT 1
        """, (f"{date_str}_%",))
        
        row = cur.fetchone()
        if row:
            last_uid = row['shipment_uid']
            try:
                # Extract suffix and increment
                last_suffix = int(last_uid.split('_')[-1])
                new_suffix = last_suffix + 1
            except ValueError:
                new_suffix = 1
        else:
            new_suffix = 1
            
        shipment_uid = f"{date_str}_{new_suffix:04d}"
        
        ref_order_number = orders[0]['order_number'] if orders else None
        
        cur.execute("""
            INSERT INTO shipments (shipment_uid, order_number, station_id, marcom_sync_status, created_at)
            VALUES (%s, %s, %s, 'PROCESSING', NOW())
            RETURNING id
        """, (shipment_uid, ref_order_number, station_id))
        
        # Link Boxes to Shipment
        if scanned_boxes:
            cur.execute("""
                UPDATE item_boxes 
                SET shipment_uid = %s 
                WHERE barcode_value = ANY(%s)
            """, (shipment_uid, scanned_boxes))
        
        conn.commit()
        
        # 5. XML
        xml_string = generate_worldship_xml({"orders": orders}, final_packages, store_number)
        filename = f"{shipment_uid}.xml"
        
        target_folder = None
        cur.execute("SELECT smb_path FROM shipping_stations WHERE station_id = %s", (station_id,))
        row = cur.fetchone()
        if row and row['smb_path']:
            target_folder = row['smb_path']
            
        if not target_folder:
            conn.close()
            return {"error": f"No valid SMB path configured for station '{station_id}'."}, 400
            
        try:
            with open(os.path.join(target_folder, filename), "w", encoding="utf-8") as f:
                f.write(xml_string)
            print(f"XML written to {target_folder}/{filename}")
        except OSError as e:
            conn.close()
            print(f"Warning: Could not write XML to {target_folder}/{filename}: {e}")
            return {"error": f"Network Error: Could not write to {target_folder}. Make sure the drive is mounted."}, 500
            
        # Assuming single tracking number for whole shipment (Worldship .out file provided it previously)
        # BUT here we are at generating the XML stage. We don't have tracking number yet?
        # Wait. The legacy app scanned Tracking Number *manually*.
        # The new app generates XML for Worldship, then Worldship prints label (getting tracking), 
        # then we parse Worldship output to get tracking.
        # SO... we CANNOT close the order with Marcom yet because we don't have the tracking number!
        # We must wait for the feedback loop (Worldship -> App -> Tracking -> Marcom).
        
        # CORRECTION: The verified plan says "After generating Worldship XML... Call marcom_service".
        # But legacy app required Tracking Number.
        # If we don't have it, we can't close it.
        
        # Update DB status to 'PENDING_TRACKING' so the feedback loop knows to pick it up?
        # Or rely on feedback_loop to trigger Marcom sync once tracking is available.
        pass

        return {"success": True, "shipment_uid": shipment_uid}, 200

    except Exception as e:
        print(e)
        if conn: conn.close()
        return {"error": str(e)}, 500

def get_recent_shipments(limit=50):
    conn = get_db_connection()
    if not conn: return [], "DB Connection Failed"
    
    try:
        cur = get_real_dict_cursor(conn)
        # Query for Shipments + Contents
        # We aggregate contents into a list
        cur.execute("""
            SELECT s.shipment_uid, s.tracking_number, s.marcom_sync_status,
                   s.marcom_response_message, s.created_at, s.packing_slip_id, s.carrier, s.order_number, s.station_id,
                   COALESCE(
                       array_agg(DISTINCT c.val) FILTER (WHERE c.val IS NOT NULL), 
                       '{}'
                   ) as contents
            FROM shipments s
            LEFT JOIN LATERAL (
                -- Priority 1: Items from Boxes (Specific to this shipment)
                SELECT i.job_ticket_display_id as val
                FROM item_boxes ib 
                JOIN items i ON ib.order_item_id = i.order_item_id
                WHERE ib.shipment_uid = s.shipment_uid
                
                UNION ALL
                
                -- Priority 2: Items from Order (Fallback if no boxes found)
                SELECT i.job_ticket_display_id as val
                FROM orders o 
                JOIN jobs j ON o.id = j.order_id
                JOIN items i ON j.id = i.job_id
                WHERE o.order_number = s.order_number
                AND NOT EXISTS (
                    SELECT 1 FROM item_boxes ib_check WHERE ib_check.shipment_uid = s.shipment_uid
                )
            ) c ON TRUE
            GROUP BY s.shipment_uid, s.tracking_number, s.marcom_sync_status, 
                     s.marcom_response_message, s.created_at, s.packing_slip_id, s.carrier, s.order_number, s.station_id
            ORDER BY s.created_at DESC
            LIMIT %s
        """, (limit,))
        
        rows = cur.fetchall()
        
        feed = []
        for r in rows:
            # Format time
            if r['created_at']:
                r['created_at'] = r['created_at'].strftime("%H:%M:%S")
            else:
                r['created_at'] = ""
            
            # Ensure fields exist
            if not r['marcom_sync_status']: r['marcom_sync_status'] = 'PENDING'
            if not r['marcom_response_message']: r['marcom_response_message'] = ''
            
            # Sort contents for display (e.g. CL123-01, CL123-02)
            if r['contents']:
                r['contents'].sort()
            
            feed.append(r)
            
        conn.close()
        return feed, None
        
    except Exception as e:
        if conn: conn.close()
        return [], str(e)
