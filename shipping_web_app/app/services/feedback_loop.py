
import os
import glob
import xml.etree.ElementTree as ET
from shared_lib.database import get_db_connection, get_real_dict_cursor

XML_DIR = 'xml_output'

def process_ups_output_files():
    """
    Reads SHIP_*.out files.
    Updates the database with the tracking number.
    Returns number of records updated.
    """
    if not os.path.exists(XML_DIR):
        return 0

    count = 0
    out_files = glob.glob(os.path.join(XML_DIR, "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_*.out"))
    
    conn = get_db_connection()
    if not conn:
        print("DB Connection failed in feedback_loop")
        return 0
        
    try:
        cur = get_real_dict_cursor(conn)
        
        for fpath in out_files:
            try:
                with open(fpath, 'r') as f:
                    content = f.read().strip()
                
                # Format: SHIP_UID,TRACKING,COST,...
                parts = content.split(',')
                if len(parts) >= 2:
                    ship_uid = parts[0].strip()
                    tracking = parts[1].strip()
                    
                    # Update DB if tracking is missing
                    cur.execute("""
                        UPDATE shipments 
                        SET tracking_number = %s
                        WHERE shipment_uid = %s AND tracking_number IS NULL
                    """, (tracking, ship_uid))
                    
                    if cur.rowcount > 0:
                        count += 1
                        
            except Exception as e:
                print(f"Error reading UPS output {fpath}: {e}")
                
        conn.commit()
    except Exception as e:
        print(f"DB Error in feedback loop (UPS): {e}")
    finally:
        conn.close()
        
    return count

def process_marcom_responses():
    """
    Reads MARCOM_CONFIRM_*.xml files.
    Updates the shipment status to SUCCESS or FAILED.
    """
    if not os.path.exists(XML_DIR):
        return 0

    count = 0
    xml_files = glob.glob(os.path.join(XML_DIR, "MARCOM_CONFIRM_*.xml"))
    
    conn = get_db_connection()
    if not conn: return 0
    
    try:
        cur = get_real_dict_cursor(conn)
        
        for fpath in xml_files:
            try:
                tree = ET.parse(fpath)
                root = tree.getroot()
                
                ref_uid = root.find('OriginalReference').text
                status = root.find('Status').text
                msg = root.find('Message').text
                
                # Append (Simulated) tag if not present
                final_msg = f"{msg} (Simulated)"
                
                # Update DB
                cur.execute("""
                    UPDATE shipments 
                    SET marcom_sync_status = %s,
                        marcom_response_message = %s
                    WHERE shipment_uid = %s AND marcom_sync_status != %s
                """, (status, final_msg, ref_uid, status))
                
                if cur.rowcount > 0:
                    count += 1
                    
            except Exception as e:
                print(f"Error reading Marcom XML {fpath}: {e}")
                
        conn.commit()
    except Exception as e:
        print(f"DB Error in feedback loop (Marcom): {e}")
    finally:
        conn.close()
        
    return count

def run_feedback_cycle():
    c1 = process_ups_output_files()
    c2 = process_marcom_responses()
    return c1, c2
