
import sys
import os
sys.path.append(os.getcwd())
from shared_lib.database import get_db_connection, get_real_dict_cursor

def test_query():
    conn = get_db_connection()
    if not conn: return

    cur = get_real_dict_cursor(conn)
    
    print("Testing LATERAL JOIN query...")
    
    query = """
        SELECT s.shipment_uid, s.tracking_number, s.marcom_sync_status,
               s.marcom_response_message, s.created_at, s.packing_slip_id,
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
                 s.marcom_response_message, s.created_at, s.packing_slip_id
        ORDER BY s.created_at DESC
        LIMIT 5
    """
    
    try:
        cur.execute(query)
        rows = cur.fetchall()
        for r in rows:
            print(f"UID: {r['shipment_uid']}")
            print(f"  Contents: {r['contents']}")
            print(f"  Status: {r['marcom_response_message']}")
            print("-" * 20)
    except Exception as e:
        print(f"Query Failed: {e}")

    conn.close()

if __name__ == "__main__":
    test_query()
