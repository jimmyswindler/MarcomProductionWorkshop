from flask import Blueprint, render_template, request, redirect, url_for
from utils.db import get_db, get_real_dict_cursor
from .api import get_chart_data

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/')
def dashboard():
    conn = get_db()
    if not conn: return "Database Error"
    cur = get_real_dict_cursor(conn)
    
    cur.execute("SELECT COUNT(*) FROM address_book")
    total_ab = cur.fetchone()['count']
    
    cur.execute("SELECT COUNT(*) FROM orders WHERE address_validation_status IN ('EXCEPTION', 'AMBIGUOUS', 'INVALID')")
    exceptions = cur.fetchone()['count']
    
    days_range = request.args.get('days', '30')
    chart_data = get_chart_data(days_range)
    
    if chart_data:
        timeline_data = chart_data['timeline_data']
    else:
        timeline_data = []
        
    chart_labels = [row['day_str'] for row in timeline_data]
    chart_orders = [row['orders_count'] for row in timeline_data]
    chart_jobs = [row['jobs_count'] for row in timeline_data]
    chart_shipments = [row['shipments_count'] for row in timeline_data]
    chart_line_items = [row['line_items_count'] for row in timeline_data]
    chart_late = [row['late_count'] for row in timeline_data]
    chart_overdue = [row['overdue_count'] for row in timeline_data]

    total_orders = sum(chart_orders)
    total_jobs = sum(chart_jobs)
    total_shipments = sum(chart_shipments)
    total_line_items = sum(chart_line_items)
    total_late = sum(chart_late)
    
    total_overdue = sum(chart_overdue) if chart_data else 0
    
    cur.execute("""
        SELECT 
            COALESCE(address_validation_status, 'NOT_VALIDATED') as status, 
            COUNT(*) as count
        FROM orders
        GROUP BY address_validation_status
    """)
    val_stats_raw = cur.fetchall()
    
    val_stats = {'VALID': 0, 'AUTO_CORRECTED': 0, 'EXCEPTION': 0, 'MANUALLY_CORRECTED': 0}
    for row in val_stats_raw:
        s = row['status']
        if s in ('EXCEPTION', 'AMBIGUOUS', 'INVALID'): val_stats['EXCEPTION'] += row['count']
        elif s in val_stats: val_stats[s] += row['count']
        elif s == 'NOT_VALIDATED': pass
        else: val_stats.setdefault('OTHER', 0); val_stats['OTHER'] += row['count']
        
    cur.execute("""
        SELECT order_number, address_validation_details 
        FROM orders 
        WHERE address_validation_status = 'MANUALLY_CORRECTED'
        ORDER BY order_date DESC
        LIMIT 10
    """)
    recent_corrections = cur.fetchall()

    cur.execute("""
        SELECT COUNT(DISTINCT o.id) as count
        FROM jobs j
        JOIN orders o ON j.order_id = o.id
        WHERE j.production_status = 'NEW'
          AND o.address_validation_status IN ('VALID', 'AUTO_CORRECTED', 'MANUALLY_CORRECTED')
    """)
    valid_orders_waiting = cur.fetchone()['count']

    # Exact logic matching `overdue.open_orders`
    cur.execute("""
        SELECT 
            o.id as order_id, 
            j.production_status,
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
    """)
    raw_overdue = cur.fetchall()
    
    unique_orders = set()
    overdue_items_count = 0
    for row in raw_overdue:
        unique_orders.add(row['order_id'])
        has_tracking = row['tracking_number'] and str(row['tracking_number']).strip() != ''
        if not has_tracking and row['production_status'] not in ('SHIPPED', 'SHIPPED_LATE', 'DELIVERED', 'CANCELLED'):
            overdue_items_count += 1
            
    open_orders_count = len(unique_orders)

    cur.close()
    conn.close()
    
    return render_template('dashboard.html', 
        total_ab=total_ab, 
        exceptions=exceptions,
        chart_labels=chart_labels,
        chart_orders=chart_orders,
        chart_jobs=chart_jobs,
        chart_shipments=chart_shipments,
        chart_line_items=chart_line_items,
        chart_late=chart_late,
        chart_overdue=chart_overdue,
        total_orders=total_orders,
        total_jobs=total_jobs,
        total_shipments=total_shipments,
        total_line_items=total_line_items,
        total_late=total_late,
        total_overdue=total_overdue,
        current_range=days_range,
        val_stats=val_stats,
        recent_corrections=recent_corrections,
        valid_orders_waiting=valid_orders_waiting,
        open_orders_count=open_orders_count,
        overdue_items_count=overdue_items_count
    )

@dashboard_bp.route('/search')
def search():
    query = request.args.get('q', '').strip()
    if not query:
        return redirect(url_for('dashboard.dashboard'))
        
    conn = get_db()
    cur = get_real_dict_cursor(conn)
    
    query_str = f'%{query}%'
    cur.execute("""
        WITH matched_orders AS (
            SELECT id as order_id FROM orders 
            WHERE order_number ILIKE %s
            UNION
            SELECT order_id FROM jobs 
            WHERE job_ticket_number ILIKE %s
            UNION
            SELECT j.order_id FROM items i
            JOIN jobs j ON i.job_id = j.id
            WHERE i.job_ticket_display_id ILIKE %s OR i.order_item_id ILIKE %s
            UNION
            SELECT DISTINCT COALESCE(s.order_id, j.order_id) as order_id
            FROM shipments s
            LEFT JOIN item_boxes ib ON ib.shipment_uid = s.shipment_uid
            LEFT JOIN items i ON i.order_item_id = ib.order_item_id
            LEFT JOIN jobs j ON j.id = i.job_id
            WHERE s.tracking_number ILIKE %s
            AND COALESCE(s.order_id, j.order_id) IS NOT NULL
        )
        SELECT 
            o.id as order_id,
            o.order_number,
            o.order_date,
            o.actual_ship_date,
            i.job_ticket_display_id AS line_item_number,
            ap.category_name AS product_category,
            COALESCE(i.print_filename, j.print_filename) AS print_filename,
            j.production_status,
            COALESCE(
               (SELECT s.tracking_number FROM shipments s 
                JOIN item_boxes ib ON ib.shipment_uid = s.shipment_uid 
                WHERE ib.order_item_id = i.order_item_id LIMIT 1),
               (SELECT s.tracking_number FROM shipments s 
                WHERE s.order_id = o.id LIMIT 1)
            ) as tracking_number
        FROM orders o
        JOIN matched_orders mo ON o.id = mo.order_id
        LEFT JOIN jobs j ON j.order_id = o.id
        LEFT JOIN items i ON i.job_id = j.id
        LEFT JOIN app_products ap ON i.product_id = ap.marcom_id
        ORDER BY o.order_date DESC, o.order_number ASC, i.job_ticket_display_id ASC
    """, (query_str, query_str, query_str, query_str, query_str))
    
    items_raw = cur.fetchall()
    
    from collections import OrderedDict
    orders_dict = OrderedDict()
    
    for row in items_raw:
        order_id = row['order_id']
        if order_id not in orders_dict:
            orders_dict[order_id] = {
                'id': row['order_id'],
                'order_number': row['order_number'],
                'order_date': row['order_date'],
                'actual_ship_date': row['actual_ship_date'],
                'line_items': []
            }
        
        if row['line_item_number'] or row['production_status']:
            orders_dict[order_id]['line_items'].append({
                'line_item_number': row['line_item_number'] or 'N/A',
                'product_category': row['product_category'] or 'Unknown',
                'print_filename': row['print_filename'] or 'N/A',
                'production_status': row['production_status'] or 'NEW',
                'tracking_number': row['tracking_number'] or ''
            })
            
    orders_list = list(orders_dict.values())
    
    cur.close()
    conn.close()
    
    return render_template('search_results.html', 
                           query=query, 
                           orders=orders_list)
