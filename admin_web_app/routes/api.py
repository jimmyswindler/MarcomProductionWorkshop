from flask import Blueprint, request
from utils.db import get_db, get_real_dict_cursor

api_bp = Blueprint('api', __name__, url_prefix='/api')

def get_chart_data(days_range):
    conn = get_db()
    if not conn: return None
    cur = get_real_dict_cursor(conn)

    if days_range == 'all':
        cur.execute("SELECT MIN(order_date) FROM orders")
        min_date = cur.fetchone()['min']
        if not min_date:
            import datetime
            min_date = datetime.date.today() - datetime.timedelta(days=30)
        else:
             if hasattr(min_date, 'date'): min_date = min_date.date()

        import datetime
        days_diff = (datetime.date.today() - min_date).days
        days_param = max(days_diff, 1)
        interval_str = f"{days_param} days"
        interval_description = "All Time"
    else:
        try:
            days_param = int(days_range)
        except:
            days_param = 30
        interval_str = f"{days_param} days"
        interval_description = f"{days_param} Days"

    cur.execute(f"""
        WITH dates AS (
            SELECT generate_series(
                CURRENT_DATE - INTERVAL '{interval_str}',
                CURRENT_DATE,
                '1 day'::interval
            )::date AS day
        ),
        daily_orders AS (
            SELECT DATE(order_date) as day, COUNT(id) as cnt
            FROM orders
            WHERE order_date >= CURRENT_DATE - INTERVAL '{interval_str}'
            GROUP BY DATE(order_date)
        ),
        daily_jobs AS (
            SELECT DATE(o.order_date) as day, COUNT(j.id) as cnt
            FROM jobs j
            JOIN orders o ON j.order_id = o.id
            WHERE o.order_date >= CURRENT_DATE - INTERVAL '{interval_str}'
            GROUP BY DATE(o.order_date)
        ),
        daily_items AS (
            SELECT DATE(o.order_date) as day, COUNT(i.id) as cnt
            FROM items i
            JOIN jobs j ON i.job_id = j.id
            JOIN orders o ON j.order_id = o.id
            WHERE o.order_date >= CURRENT_DATE - INTERVAL '{interval_str}'
            GROUP BY DATE(o.order_date)
        ),
        daily_shipments AS (
            SELECT DATE(ship_date) as day, COUNT(id) as cnt
            FROM shipments
            WHERE ship_date >= CURRENT_DATE - INTERVAL '{interval_str}'
            GROUP BY DATE(ship_date)
        ),
        daily_late AS (
            SELECT DATE(actual_ship_date) as day, COUNT(id) as cnt
            FROM orders
            WHERE actual_ship_date >= CURRENT_DATE - INTERVAL '{interval_str}'
              AND actual_ship_date > ship_date
            GROUP BY DATE(actual_ship_date)
        ),
        daily_overdue AS (
            SELECT DATE(o.ship_date) as day, COUNT(i.id) as cnt
            FROM items i
            JOIN jobs j ON i.job_id = j.id
            JOIN orders o ON j.order_id = o.id
            WHERE o.ship_date >= CURRENT_DATE - INTERVAL '{interval_str}'
              AND o.actual_ship_date IS NULL
            GROUP BY DATE(o.ship_date)
        )
        SELECT 
            TO_CHAR(d.day, 'YYYY-MM-DD') as day_str,
            COALESCE(cte_orders.cnt, 0) as orders_count,
            COALESCE(cte_jobs.cnt, 0) as jobs_count,
            COALESCE(cte_shipments.cnt, 0) as shipments_count,
            COALESCE(cte_items.cnt, 0) as line_items_count,
            COALESCE(cte_late.cnt, 0) as late_count,
            COALESCE(cte_overdue.cnt, 0) as overdue_count
        FROM dates d
        LEFT JOIN daily_orders cte_orders ON cte_orders.day = d.day
        LEFT JOIN daily_jobs cte_jobs ON cte_jobs.day = d.day
        LEFT JOIN daily_shipments cte_shipments ON cte_shipments.day = d.day
        LEFT JOIN daily_items cte_items ON cte_items.day = d.day
        LEFT JOIN daily_late cte_late ON cte_late.day = d.day
        LEFT JOIN daily_overdue cte_overdue ON cte_overdue.day = d.day
        ORDER BY d.day ASC
    """)
    timeline_data = cur.fetchall()
    
    cur.execute("""
        SELECT COUNT(i.id) as cnt 
        FROM items i
        JOIN jobs j ON i.job_id = j.id
        JOIN orders o ON j.order_id = o.id
        WHERE o.actual_ship_date IS NULL AND o.ship_date < CURRENT_DATE
    """)
    global_overdue = cur.fetchone()['cnt']
    
    cur.close()
    conn.close()

    return {
        'timeline_data': timeline_data,
        'interval_description': interval_description,
        'global_overdue': global_overdue
    }

@api_bp.route('/chart-data')
def api_chart_data():
    days_range = request.args.get('days', '30')
    data = get_chart_data(days_range)
    
    if not data:
        return {"error": "Database Error"}, 500

    timeline_data = data['timeline_data']
    
    chart_labels = [row['day_str'] for row in timeline_data]
    chart_orders = [row['orders_count'] for row in timeline_data]
    chart_jobs = [row['jobs_count'] for row in timeline_data]
    chart_shipments = [row['shipments_count'] for row in timeline_data]
    chart_line_items = [row['line_items_count'] for row in timeline_data]
    chart_late = [row['late_count'] for row in timeline_data]
    chart_overdue = [row['overdue_count'] for row in timeline_data]

    totals = {
        'orders': sum(chart_orders),
        'jobs': sum(chart_jobs),
        'shipments': sum(chart_shipments),
        'line_items': sum(chart_line_items),
        'late': sum(chart_late),
        'overdue': data.get('global_overdue', sum(chart_overdue)) 
    }

    return {
        'labels': chart_labels,
        'datasets': {
            'orders': chart_orders,
            'jobs': chart_jobs,
            'shipments': chart_shipments,
            'line_items': chart_line_items,
            'late': chart_late,
            'overdue': chart_overdue
        },
        'totals': totals,
        'interval_description': data['interval_description']
    }

@api_bp.route('/address-book/<store_number>')
def api_address_book(store_number):
    conn = get_db()
    if not conn:
        return {"error": "Database Error"}, 500
        
    cur = get_real_dict_cursor(conn)
    if store_number.isdigit():
         store_number = store_number.zfill(4)
         
    cur.execute("SELECT * FROM address_book WHERE store_number = %s", (store_number,))
    addr = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if addr:
        return addr
    else:
         return {"error": "Not Found"}, 404
