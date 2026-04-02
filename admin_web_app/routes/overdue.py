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
            o.id, o.order_number, o.order_date, o.ship_date, j.production_status
        FROM items i
        JOIN jobs j ON i.job_id = j.id
        JOIN orders o ON j.order_id = o.id
        WHERE o.actual_ship_date IS NULL
          AND o.ship_date < CURRENT_DATE
        ORDER BY o.ship_date ASC, i.job_ticket_display_id ASC
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
            'production_status': row['production_status']
        })
        
    orders_list = list(orders_dict.values())
    open_items_count = len(items_raw)
    
    cur.close()
    conn.close()
    
    return render_template('open_orders.html', orders=orders_list, open_items_count=open_items_count)
