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
            o.id, o.order_number, o.order_date, o.ship_date, o.ship_to_company, 
            o.city, o.state, o.store_number
        FROM items i
        JOIN jobs j ON i.job_id = j.id
        JOIN orders o ON j.order_id = o.id
        WHERE o.actual_ship_date IS NULL
          AND o.ship_date < CURRENT_DATE
        ORDER BY o.ship_date ASC, i.job_ticket_display_id ASC
    """)
    items_raw = cur.fetchall()
    
    items = []
    for item_dict in items_raw:
        item = dict(item_dict)
        if item.get('store_number') and str(item['store_number']).isdigit():
            item['store_number'] = str(item['store_number']).zfill(4)
        items.append(item)
    
    cur.close()
    conn.close()
    
    return render_template('open_orders.html', items=items)
