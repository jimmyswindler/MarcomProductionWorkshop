
import math
import os
import datetime
import random
from shared_lib.database import get_db_connection, get_real_dict_cursor
from shared_lib.config import get_env_var
from shared_lib.utils import get_store_number

XML_OUTPUT_FOLDER = 'xml_output'
# Ensure absolute path relative to root if running from root
if not os.path.exists(XML_OUTPUT_FOLDER):
    os.makedirs(XML_OUTPUT_FOLDER)

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

    if 1 <= store_int <= 1000:
        final_company = f"Texas Roadhouse #{store_number_str.lstrip('0')}"
        final_attention = "Store Manager"
    else:
        final_company = ship_to.get('company', '')
        final_attention = ship_to.get('name', '')
    
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
        weight_raw = float(pkg.get('weight', 1.0))
        weight_int = int(math.ceil(weight_raw))
        
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

def process_shipment_logic(orders, scanned_boxes, package_list_in):
    conn = get_db_connection()
    if not conn: raise Exception("DB Connection Failed")
    
    try:
        cur = get_real_dict_cursor(conn)
        
        # 1a. Partial Check
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
                    conn.close()
                    return {"error": f"Partial Line Item detected for SKU {row['sku']}. Scan all boxes."}, 400

        # 1. Update Box Status
        if scanned_boxes:
            cur.execute("""
                UPDATE item_boxes 
                SET status = 'packed', packed_at = NOW()
                WHERE barcode_value = ANY(%s)
            """, (scanned_boxes,))

        # 2. Calculate Weights (Simplified for now, similar to original)
        cur.execute("SELECT category_name, quantity, box_weight FROM product_shipping_rules")
        rules = {(r['category_name'], r['quantity']): r['box_weight'] for r in cur.fetchall()}
        
        cur.execute("SELECT code, weight, length, width, height FROM shipping_cartons")
        cartons = {c['code']: c for c in cur.fetchall()}
        
        total_shipment_product_weight = 0.0
        store_number = None
        
        if scanned_boxes:
             cur.execute("""
                SELECT i.quantity_ordered, i.cost_center 
                FROM item_boxes b
                JOIN items i ON b.order_item_id = i.order_item_id
                WHERE b.barcode_value = ANY(%s)
             """, (scanned_boxes,))
             for row in cur.fetchall():
                 q = row['quantity_ordered']
                 cat = row['cost_center']
                 w = rules.get((cat, q), 1.0)
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
                     weight = total_shipment_product_weight + carton_data['weight']
            
            final_packages.append({"weight": round(weight, 1), **dims})

        # 4. Generate Shipment
        timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        rand_suffix = str(random.randint(1000, 9999))
        shipment_uid = f"SHIP_{timestamp_str}_{rand_suffix}"
        
        ref_order_number = orders[0]['order_number'] if orders else None
        
        cur.execute("""
            INSERT INTO shipments (shipment_uid, job_ticket_number, marcom_sync_status, created_at)
            VALUES (%s, %s, 'PROCESSING', NOW())
            RETURNING id
        """, (shipment_uid, ref_order_number))
        
        conn.commit()
        cur.close()
        conn.close()
        
        # 5. XML
        xml_string = generate_worldship_xml({"orders": orders}, final_packages, store_number)
        filename = f"{shipment_uid}.xml"
        with open(os.path.join(XML_OUTPUT_FOLDER, filename), "w") as f:
            f.write(xml_string)
            
        return {"success": True, "shipment_uid": shipment_uid}, 200

    except Exception as e:
        print(e)
        if conn: conn.close()
        return {"error": str(e)}, 500
