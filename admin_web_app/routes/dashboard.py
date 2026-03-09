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
    
    total_overdue = chart_data.get('global_overdue', sum(chart_overdue)) if chart_data else 0
    
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
        valid_orders_waiting=valid_orders_waiting
    )

@dashboard_bp.route('/search')
def search():
    query = request.args.get('q', '').strip()
    if not query:
        return redirect(url_for('dashboard.dashboard'))
        
    conn = get_db()
    cur = get_real_dict_cursor(conn)
    
    cur.execute("""
        SELECT * FROM orders 
        WHERE order_number ILIKE %s 
           OR ship_to_company ILIKE %s 
           OR ship_to_name ILIKE %s
        ORDER BY order_date DESC LIMIT 20
    """, (f'%{query}%', f'%{query}%', f'%{query}%'))
    found_orders = cur.fetchall()
    
    cur.execute("""
        SELECT j.*, o.order_number FROM jobs j
        JOIN orders o ON j.order_id = o.id
        WHERE j.job_ticket_number ILIKE %s
        ORDER BY j.id DESC LIMIT 20
    """, (f'%{query}%',))
    found_jobs = cur.fetchall()
    
    cur.execute("""
        SELECT s.*, o.order_number as order_num_from_orders FROM shipments s
        LEFT JOIN orders o ON s.order_number = o.order_number
        WHERE s.tracking_number ILIKE %s
        ORDER BY s.created_at DESC LIMIT 20
    """, (f'%{query}%',))
    found_shipments = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return render_template('search_results.html', 
                           query=query, 
                           found_orders=found_orders, 
                           found_jobs=found_jobs, 
                           found_shipments=found_shipments)
