
import os
import glob
import xml.etree.ElementTree as ET
from shared_lib.database import get_db_connection, get_real_dict_cursor
from . import marcom_service
from . import shipment_service


# Directory paths

def ensure_processed_dir(base_dir):
    """Ensures the 'processed' subdirectory exists."""
    processed_dir = os.path.join(base_dir, 'processed')
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)
    return processed_dir

def move_to_processed(file_path, base_dir):
    """Moves a file to the 'processed' subdirectory."""
    try:
        processed_dir = ensure_processed_dir(base_dir)
        filename = os.path.basename(file_path)
        dest_path = os.path.join(processed_dir, filename)
        
        # If destination exists, overwrite or rename? 
        # Overwrite is safer to avoid clutter, as we've processed it.
        os.replace(file_path, dest_path)
        print(f"Archived {filename} to processed/")
    except Exception as e:
        print(f"Failed to move {file_path}: {e}")

def process_ups_output_files():
    """
    Reads SHIP_*.out files from all active stations.
    Updates the database with the tracking number.
    Returns number of records updated.
    """
    conn = get_db_connection()
    if not conn:
        print("DB Connection failed in feedback_loop")
        return 0

    target_dirs = []
    try:
        cur = get_real_dict_cursor(conn)
        cur.execute("SELECT smb_path FROM shipping_stations WHERE is_active = TRUE")
        for row in cur.fetchall():
            if row['smb_path']:
                target_dirs.append(row['smb_path'])
    except Exception as e:
        print(f"Error fetching stations in feedback loop: {e}")

    count = 0
    try:
        cur = get_real_dict_cursor(conn)
        
        for target_dir in target_dirs:
            if not os.path.exists(target_dir):
                continue

            processed_dir = ensure_processed_dir(target_dir)

            out_files = glob.glob(os.path.join(target_dir, "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_*.out"))
            if not out_files:
                 out_files = glob.glob(os.path.join(target_dir, "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_*.Out"))
                 
            for fpath in out_files:
                try:
                    # Derive shipment_uid from filename regardless of format
                    # Filename example: 20260212_0011.Out -> 20260212_0011
                    fname = os.path.basename(fpath)
                    ship_uid_from_file = os.path.splitext(fname)[0]

                    with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read().strip()
                
                    tracking = None
                
                    # Check if XML
                    if content.strip().startswith('<') and 'OpenShipments' in content:
                        try:
                            # Parse XML to find tracking number
                            import re
                            xml_clean = re.sub(r' xmlns="[^"]+"', '', content, count=1)
                            root = ET.fromstring(xml_clean)
                            tn_node = root.find(".//TrackingNumber")
                            if tn_node is not None:
                                tracking = tn_node.text
                            
                        except Exception as e:
                            print(f"Failed to parse XML in {fpath}: {e}")
                
                    # Fallback to CSV (legacy/simulation)
                    if not tracking:
                        parts = content.split(',')
                        if len(parts) >= 2:
                            # In CSV, col 0 is ship_uid, col 1 is tracking
                            csv_uid = parts[0].strip()
                            if csv_uid == ship_uid_from_file:
                                 tracking = parts[1].strip()
                            else:
                                 tracking = parts[1].strip()

                    if tracking:
                        # Update DB if tracking is missing
                        # Force status to PENDING so it gets picked up by sync loop, 
                        # unless it's already success/failed? No, if we are setting key, we want to try sync.
                        cur.execute("""
                            UPDATE shipments 
                            SET tracking_number = %s,
                                marcom_sync_status = 'PENDING',
                                ship_date = COALESCE(ship_date, NOW())
                            WHERE shipment_uid = %s AND tracking_number IS NULL
                            RETURNING order_number, ship_date
                        """, (tracking, ship_uid_from_file))
                    
                        if cur.rowcount > 0:
                            count += 1
                            print(f"Updated tracking for {ship_uid_from_file}: {tracking}")
                        
                            row = cur.fetchone()
                            if row:
                                order_num = row['order_number']
                                ship_date = row['ship_date']
                            
                                if order_num:
                                    cur.execute("""
                                        UPDATE orders
                                        SET actual_ship_date = %s
                                        WHERE order_number = %s AND actual_ship_date IS NULL
                                    """, (ship_date, order_num))
                            
                                cur.execute("""
                                    UPDATE jobs
                                    SET production_status = 'SHIPPED'
                                    WHERE id IN (
                                        SELECT i.job_id
                                        FROM item_boxes b
                                        JOIN items i ON b.order_item_id = i.order_item_id
                                        WHERE b.shipment_uid = %s
                                    ) AND production_status != 'SHIPPED'
                                """, (ship_uid_from_file,))
                    else:
                        print(f"Could not extract tracking number from {fpath}")
                        # Update DB to show error in UI
                        cur.execute("""
                            UPDATE shipments
                            SET marcom_response_message = %s
                            WHERE shipment_uid = %s AND tracking_number IS NULL
                        """, (f"UPS Error: File found but no tracking in {os.path.basename(fpath)}", ship_uid_from_file))
                        
                except Exception as e:
                    print(f"Error reading UPS output {fpath}: {e}")
            
                # ALWAYS move the file to processed, even if it failed parsing or was legacy/ignored.
                # This prevents infinite loops of trying to read bad files.
                move_to_processed(fpath, target_dir)
                
        conn.commit()
        
        # --- SYNC STEP ---
        # Find ANY shipment that has tracking but is PENDING or FAILED sync
        process_pending_marcom_syncs(conn)

    except Exception as e:
        print(f"DB Error in feedback loop (UPS): {e}")
    finally:
        conn.close()
        
    return count

def process_pending_marcom_syncs(conn):
    """
    Finds shipments that have a tracking number but have not successfully synced to Marcom.
    """
    try:
        cur = get_real_dict_cursor(conn)
        
        # 1. Find shipments needing sync (PENDING, failed, partial).
        # We explicitly EXCLUDE 'PROCESSING' here so a concurrent run doesn't even see it.
        cur.execute("""
            SELECT shipment_uid, tracking_number, order_number 
            FROM shipments 
            WHERE tracking_number IS NOT NULL 
              AND (marcom_sync_status IS NULL 
                   OR marcom_sync_status = 'PENDING' 
                   OR marcom_sync_status = 'PARTIAL_FAIL')
            ORDER BY created_at DESC
            LIMIT 10
        """)
        
        shipments_to_sync = cur.fetchall()
        
        for ship in shipments_to_sync:
            ship_uid = ship['shipment_uid']
            tracking = ship['tracking_number']
            order_num = ship['order_number']
            
            # 2. Attempt to explicitly lock/claim this shipment
            cur.execute("""
                UPDATE shipments 
                SET marcom_sync_status = 'PROCESSING'
                WHERE shipment_uid = %s 
                  AND (marcom_sync_status IS NULL 
                       OR marcom_sync_status = 'PENDING' 
                       OR marcom_sync_status = 'PARTIAL_FAIL')
            """, (ship_uid,))
            
            # If rowcount is 0, another process already claimed it between our SELECT and UPDATE.
            if cur.rowcount == 0:
                print(f"Skipping {ship_uid} - already claimed by another process.")
                continue
                
            # Commit the lock immediately so other processes see 'PROCESSING'
            conn.commit()
            
            print(f"Processing Pending Sync for {ship_uid}...")
            sync_shipment_to_marcom(cur, ship_uid, tracking, order_num)
            conn.commit() # Commit after each sync to save progress

        cur.close()
        
    except Exception as e:
        print(f"Error in process_pending_marcom_syncs: {e}")

def sync_shipment_to_marcom(cur, ship_uid, tracking, order_number=None):
    # 1. Get Line Items for this shipment (DISTINCT to avoid duplicates if split across boxes)
    cur.execute("""
        SELECT DISTINCT i.order_item_id, i.sku
        FROM item_boxes b
        JOIN items i ON b.order_item_id = i.order_item_id
        WHERE b.shipment_uid = %s
    """, (ship_uid,))
    items = cur.fetchall()
    
    # Fallback: If no boxes linked, try fallback via order_number (Legacy/Bulk Import Support)
    if not items and order_number:
        print(f"No item_boxes found for {ship_uid}, trying fallback via Order {order_number}")
        cur.execute("""
            SELECT DISTINCT i.order_item_id, i.sku
            FROM orders o 
            JOIN jobs j ON o.id = j.order_id
            JOIN items i ON j.id = i.job_id
            WHERE o.order_number = %s
        """, (order_number,))
        items = cur.fetchall()
    
    if not items:
        # Check if items exist but maybe linked via legacy method?
        # For now, just warn.
        print(f"Warning: No items found for shipment {ship_uid}")
        # Mark as FAILED to prevent infinite retry loop, but user can reset it
        cur.execute("UPDATE shipments SET marcom_sync_status = 'FAILED', marcom_response_message = 'No items found' WHERE shipment_uid = %s", (ship_uid,))
        return

    overall_status = "SUCCESS"
    messages = []
    last_slip_id = None
    
    for item in items:
        # 2. Call Marcom API
        print(f"Sending Packing Slip for Item {item['order_item_id']}...")
        resp = marcom_service.send_packing_slip(item['order_item_id'], tracking)
        
        if resp['success']:
            code = resp.get('code')
            if code:
                messages.append(f"Item {item['order_item_id']}: Code: {code}, {resp['message']}")
            else:
                messages.append(f"Item {item['order_item_id']}: OK (Slip {resp['packing_slip_id']})")
            
            last_slip_id = resp['packing_slip_id']
        else:
            code = resp.get('code')
            if code:
                messages.append(f"Item {item['order_item_id']}: Code: {code}, {resp['message']}")
            else:
                messages.append(f"Item {item['order_item_id']}: {resp['status']} - {resp['message']}")

            print(f"Marcom Error: {resp['message']}")
    
    final_msg = "; ".join(messages)
    
    # 3. Update Status
    cur.execute("""
        UPDATE shipments 
        SET marcom_sync_status = %s,
            marcom_response_message = %s,
            packing_slip_id = %s
        WHERE shipment_uid = %s
    """, (overall_status, final_msg, last_slip_id, ship_uid))
    print(f"Marcom Sync Complete for {ship_uid}. Status: {overall_status}. Details: {final_msg}")

    print(f"Marcom Sync Complete for {ship_uid}. Status: {overall_status}. Details: {final_msg}")
