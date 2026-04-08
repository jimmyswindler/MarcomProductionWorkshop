from datetime import datetime
from flask import Blueprint, render_template
from utils.db import get_db, get_real_dict_cursor

overdue_bp = Blueprint('overdue', __name__)

@overdue_bp.route('/open-orders')
def open_orders():
    conn = get_db()
    if not conn: return "Database Error"
    cur = get_real_dict_cursor(conn)

    cur.execute("""
        SELECT 
            i.job_ticket_display_id AS line_item_number,
            ap.category_name AS product_category,
            o.id, o.order_number, o.order_date, o.ship_date, j.production_status,
            COALESCE(
               (SELECT s.tracking_number FROM shipments s 
                JOIN item_boxes ib ON ib.shipment_uid = s.shipment_uid 
                WHERE ib.order_item_id = i.order_item_id LIMIT 1),
               (SELECT s.tracking_number FROM shipments s 
                WHERE s.order_id = o.id LIMIT 1)
            ) as tracking_number
        FROM items i
        JOIN jobs j ON i.job_id = j.id
        JOIN orders o ON j.order_id = o.id
        LEFT JOIN app_products ap ON i.product_id = ap.marcom_id
        WHERE o.id IN (
            SELECT DISTINCT j_sub.order_id
            FROM items i_sub
            JOIN jobs j_sub ON i_sub.job_id = j_sub.id
            JOIN orders o_sub ON j_sub.order_id = o_sub.id
            LEFT JOIN item_boxes ib_sub ON ib_sub.order_item_id = i_sub.order_item_id
            LEFT JOIN shipments s_sub ON (s_sub.shipment_uid = ib_sub.shipment_uid OR s_sub.order_id = o_sub.id)
            WHERE j_sub.production_status NOT IN ('SHIPPED', 'SHIPPED_LATE')
              AND o_sub.ship_date < CURRENT_DATE
              AND (s_sub.tracking_number IS NULL OR TRIM(s_sub.tracking_number) = '')
        )
        ORDER BY o.ship_date ASC, o.order_number ASC, i.job_ticket_display_id ASC
    """)
    items_raw = cur.fetchall()
    
    from collections import OrderedDict
    orders_dict = OrderedDict()
    today = datetime.now().date()
    
    for row in items_raw:
        order_id = row['id']
        if order_id not in orders_dict:
            ship_date = row['ship_date']
            if ship_date:
                days_overdue = (today - ship_date.date()).days
                days_overdue = days_overdue if days_overdue > 0 else 0
            else:
                days_overdue = 0
                
            orders_dict[order_id] = {
                'id': row['id'],
                'order_number': row['order_number'],
                'order_date': row['order_date'],
                'ship_date': ship_date,
                'days_overdue': days_overdue,
                'line_items': []
            }
        
        # Add item specific status
        orders_dict[order_id]['line_items'].append({
            'line_item_number': row['line_item_number'],
            'product_category': row['product_category'] or 'Unknown',
            'production_status': row['production_status'],
            'tracking_number': row['tracking_number'] or ''
        })
        
    orders_list = list(orders_dict.values())
    
    # Calculate unique orders and strict overdue items
    open_orders_count = len(orders_list)
    overdue_items_count = 0
    for row in items_raw:
        has_tracking = row['tracking_number'] and str(row['tracking_number']).strip() != ''
        if not has_tracking and row['production_status'] not in ('SHIPPED', 'SHIPPED_LATE', 'DELIVERED', 'CANCELLED'):
            overdue_items_count += 1
    
    cur.close()
    conn.close()
    
    return render_template('open_orders.html', 
                         orders=orders_list, 
                         open_orders_count=open_orders_count, 
                         overdue_items_count=overdue_items_count)
