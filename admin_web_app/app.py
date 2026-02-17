from flask import Flask, render_template, request, redirect, url_for, flash
import psycopg2
import psycopg2.extras
import os
import math
from dotenv import load_dotenv

# Load env from root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(project_root, '.env'))

app = Flask(__name__)
app.secret_key = 'admin_secret_key_change_in_prod'

DB_NAME = os.getenv("DB_NAME", "marcom_production_suite")
DB_USER = os.getenv("DB_USER", "jimmyswindler")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, host=DB_HOST, port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"DB Error: {e}")
        return None


def get_chart_data(days_range):
    conn = get_db_connection()
    if not conn: return None
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if days_range == 'all':
        # Find min date
        cur.execute("SELECT MIN(order_date) FROM orders")
        min_date = cur.fetchone()['min']
        if not min_date:
            import datetime
            min_date = datetime.date.today() - datetime.timedelta(days=30)
        else:
             # Ensure it's a date object
             if hasattr(min_date, 'date'): min_date = min_date.date()

        # Build series from min_date to yesterday
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
                CURRENT_DATE - INTERVAL '1 day',
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
            SELECT DATE(ship_date) as day, COUNT(id) as cnt
            FROM orders
            WHERE ship_date >= CURRENT_DATE - INTERVAL '{interval_str}'
              AND actual_ship_date IS NULL
            GROUP BY DATE(ship_date)
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
    
    # Calculate Global Overdue (All Time Backlog)
    cur.execute("SELECT COUNT(*) as cnt FROM orders WHERE actual_ship_date IS NULL AND ship_date < CURRENT_DATE")
    global_overdue = cur.fetchone()['cnt']
    
    cur.close()
    conn.close()

    return {
        'timeline_data': timeline_data,
        'interval_description': interval_description,
        'global_overdue': global_overdue
    }

@app.route('/api/chart-data')
def api_chart_data():
    days_range = request.args.get('days', '30')
    data = get_chart_data(days_range)
    
    if not data:
        return {"error": "Database Error"}, 500

    timeline_data = data['timeline_data']
    
    # Format for Chart.js
    chart_labels = [row['day_str'] for row in timeline_data]
    chart_orders = [row['orders_count'] for row in timeline_data]
    chart_jobs = [row['jobs_count'] for row in timeline_data]
    chart_shipments = [row['shipments_count'] for row in timeline_data]
    chart_line_items = [row['line_items_count'] for row in timeline_data]
    chart_late = [row['late_count'] for row in timeline_data]
    chart_overdue = [row['overdue_count'] for row in timeline_data]

    # Calculate Totals
    # Use global_overdue from data for the 'overdue' total, so it matches the backlog list
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

@app.route('/')
def dashboard():
    conn = get_db_connection()
    if not conn: return "Database Error"
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    # Stats
    cur.execute("SELECT COUNT(*) FROM address_book")
    total_ab = cur.fetchone()['count']
    
    cur.execute("SELECT COUNT(*) FROM orders WHERE address_validation_status IN ('AMBIGUOUS', 'INVALID')")
    exceptions = cur.fetchone()['count']
    
    # --- CHART DATA ---
    days_range = request.args.get('days', '30')
    chart_data = get_chart_data(days_range)
    
    if chart_data:
        timeline_data = chart_data['timeline_data']
        interval_description = chart_data['interval_description']
    else:
        timeline_data = [] # Handle error gracefully
        interval_description = "Error"
        
    # Format for Chart.js
    chart_labels = [row['day_str'] for row in timeline_data]
    chart_orders = [row['orders_count'] for row in timeline_data]
    chart_jobs = [row['jobs_count'] for row in timeline_data]
    chart_shipments = [row['shipments_count'] for row in timeline_data]
    chart_line_items = [row['line_items_count'] for row in timeline_data]
    chart_late = [row['late_count'] for row in timeline_data]
    chart_overdue = [row['overdue_count'] for row in timeline_data]

    # Calculate Totals for Legend
    total_orders = sum(chart_orders)
    total_jobs = sum(chart_jobs)
    total_shipments = sum(chart_shipments)
    total_line_items = sum(chart_line_items)
    total_late = sum(chart_late)
    
    # Use global overdue count from helper
    total_overdue = chart_data.get('global_overdue', sum(chart_overdue)) if chart_data else 0
    
    # --- VALIDATION STATS ---
    cur.execute("""
        SELECT 
            COALESCE(address_validation_status, 'NOT_VALIDATED') as status, 
            COUNT(*) as count
        FROM orders
        GROUP BY address_validation_status
    """)
    val_stats_raw = cur.fetchall()
    
    # Normalize for UI
    val_stats = {'VALID': 0, 'CORRECTED': 0, 'AMBIGUOUS': 0, 'INVALID': 0, 'MANUALLY_CORRECTED': 0, 'CORRECTED_BY_BOOK': 0}
    for row in val_stats_raw:
        s = row['status']
        if s in val_stats: val_stats[s] += row['count']
        elif s == 'NOT_VALIDATED': pass # Ignore for report? or add separate?
        else: val_stats.setdefault('OTHER', 0); val_stats['OTHER'] += row['count']
        
    # --- RECENT AUTO-CORRECTIONS ---
    cur.execute("""
        SELECT order_number, address_validation_details 
        FROM orders 
        WHERE address_validation_status IN ('CORRECTED', 'CORRECTED_BY_BOOK', 'MANUALLY_CORRECTED')
        ORDER BY order_date DESC
        LIMIT 10
    """)
    recent_corrections = cur.fetchall()

    # --- OPEN ORDERS (Overdue) ---
    # Moved to separate page /open-orders
    # We still need the count for the dashboard card, which is covered by total_overdue
    
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
        # Totals for Legend
        total_orders=total_orders,
        total_jobs=total_jobs,
        total_shipments=total_shipments,
        total_line_items=total_line_items,
        total_late=total_late,
        total_overdue=total_overdue,
        # Range
        current_range=days_range,
        # Stats
        val_stats=val_stats,
        recent_corrections=recent_corrections
    )

@app.route('/open-orders')
def open_orders():
    conn = get_db_connection()
    if not conn: return "Database Error"
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # --- OPEN ORDERS (Overdue) ---
    # Show any (order, Job, Item) where 'Actual Ship Date' is empty, 
    # and the 'previously calculated ship date' is in the past.
    cur.execute("""
        SELECT 
            id, order_number, order_date, ship_date, ship_to_company, 
            city, state
        FROM orders
        WHERE actual_ship_date IS NULL
          AND ship_date < CURRENT_DATE
        ORDER BY ship_date ASC
    """)
    orders = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return render_template('open_orders.html', orders=orders)

@app.route('/search')
def search():
    query = request.args.get('q', '').strip()
    if not query:
        return redirect(url_for('dashboard'))
        
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    # Search Orders
    cur.execute("""
        SELECT * FROM orders 
        WHERE order_number ILIKE %s 
           OR ship_to_company ILIKE %s 
           OR ship_to_name ILIKE %s
        ORDER BY order_date DESC LIMIT 20
    """, (f'%{query}%', f'%{query}%', f'%{query}%'))
    found_orders = cur.fetchall()
    
    # Search Jobs
    cur.execute("""
        SELECT j.*, o.order_number FROM jobs j
        JOIN orders o ON j.order_id = o.id
        WHERE j.job_ticket_number ILIKE %s
        ORDER BY j.id DESC LIMIT 20
    """, (f'%{query}%',))
    found_jobs = cur.fetchall()
    
    # Search Shipments
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

@app.route('/address-book')
def address_book():
    search = request.args.get('search', '')
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    if search:
        cur.execute("SELECT * FROM address_book WHERE store_number ILIKE %s OR company_name ILIKE %s ORDER BY store_number", (f'%{search}%', f'%{search}%'))
    else:
        cur.execute("SELECT * FROM address_book ORDER BY store_number LIMIT 100") # Pagination TODO
    
    addresses = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('address_book.html', addresses=addresses, search=search)

@app.route('/address-book/edit/<store_number>', methods=['GET', 'POST'])
def edit_address(store_number):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    if request.method == 'POST':
        # Update
        sql = """
            UPDATE address_book SET
                company_name = %s, attn = %s,
                address1 = %s, address2 = %s, address3 = %s,
                city = %s, state = %s, zip = %s,
                last_updated = NOW()
            WHERE store_number = %s
        """
        cur.execute(sql, (
            request.form['company_name'], request.form['attn'],
            request.form['address1'], request.form['address2'], request.form['address3'],
            request.form['city'], request.form['state'], request.form['zip'],
            store_number
        ))
        conn.commit()
        cur.close()
        conn.close()
        flash('Address Updated', 'success')
        return redirect(url_for('address_book'))
        
    cur.execute("SELECT * FROM address_book WHERE store_number = %s", (store_number,))
    addr = cur.fetchone()
    cur.close()
    conn.close()
    return render_template('edit_address.html', addr=addr)

@app.route('/exceptions')
def exceptions():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    cur.execute("""
        SELECT * FROM orders 
        WHERE address_validation_status IN ('AMBIGUOUS', 'INVALID') 
        ORDER BY order_date DESC
    """)
    orders = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('exceptions.html', orders=orders)

@app.route('/exceptions/fix/<order_number>', methods=['GET', 'POST'])
def fix_exception(order_number):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'manual_fix':
            # Manual Edit
            sql = """
                UPDATE orders SET
                    address1 = %s, address2 = %s, city = %s, state = %s, zip = %s,
                    address_validated = TRUE,
                    address_validation_status = 'MANUALLY_CORRECTED',
                    address_validation_details = %s
                WHERE order_number = %s
            """
            import json
            details = json.dumps({'msg': 'Manually corrected via Admin UI'})
            cur.execute(sql, (
                request.form['address1'], request.form['address2'], 
                request.form['city'], request.form['state'], request.form['zip'],
                details, order_number
            ))
            conn.commit()
            flash(f'Order {order_number} corrected manually.', 'success')
            
        elif action == 'apply_book':
            # Apply from Address Book
            store_key = request.form.get('store_number', '').strip()
            
            # Normalize key
            if store_key.isdigit(): store_key = store_key.zfill(4)
            
            cur.execute("SELECT * FROM address_book WHERE store_number = %s", (store_key,))
            book_entry = cur.fetchone()
            
            if book_entry:
                sql = """
                    UPDATE orders SET
                        address1 = %s, address2 = %s, address3 = %s,
                        city = %s, state = %s, zip = %s,
                        address_validated = TRUE,
                        address_validation_status = 'CORRECTED_BY_BOOK',
                        address_validation_details = %s
                    WHERE order_number = %s
                """
                import json
                details = json.dumps({'msg': 'Applied from Address Book via Admin UI', 'store_number': store_key})
                cur.execute(sql, (
                    book_entry['address1'], book_entry['address2'], book_entry['address3'],
                    book_entry['city'], book_entry['state'], book_entry['zip'],
                    details, order_number
                ))
                conn.commit()
                flash(f'Order {order_number} corrected using Store #{store_key}.', 'success')
            else:
                flash(f'Store #{store_key} not found in Address Book.', 'danger')
                return redirect(url_for('fix_exception', order_number=order_number))

        cur.close()
        conn.close()
        return redirect(url_for('exceptions'))

    # GET Request - Show Form
    cur.execute("SELECT * FROM orders WHERE order_number = %s", (order_number,))
    order = cur.fetchone()
    
    # Normalize valid store number for display
    if order.get('store_number') and str(order['store_number']).isdigit():
         # We need to update the dict (RealDictCursor returns dict-like)
         # But it might be immutable or separate.
         # Let's create a display copy or update if mutable.
         # RealDictRow is somewhat mutable? Or convert to dict?
         order = dict(order)
         order['store_number'] = str(order['store_number']).zfill(4)

    # Try to guess store number for pre-fill
    guessed_store = order.get('store_number')
    if not guessed_store:
        import re
        # Try extracting from ShipToCompany
        match = re.search(r'#(\d+)', order.get('ship_to_company', ''))
        if match: 
            guessed_store = match.group(1)
    
    # Normalize if found
    if guessed_store and str(guessed_store).isdigit():
        guessed_store = str(guessed_store).zfill(4)

    cur.close()
    conn.close()
    return render_template('fix_exception.html', order=order, guessed_store=guessed_store)

@app.route('/production-review')
def production_review():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    # Pagination Logic
    page = request.args.get('page', 1, type=int)
    per_page = 20
    offset = (page - 1) * per_page

    # Base WHERE clause
    # Fetch NEW jobs where order is Validated
    where_clause = """
        WHERE j.production_status = 'NEW'
          AND o.address_validation_status IN ('VALID', 'CORRECTED', 'CORRECTED_BY_BOOK', 'MANUALLY_CORRECTED')
    """
    
    # --- METRICS ---
    # Calculate counts for the dashboard/review page summary
    # Total Orders, Jobs, Items waiting
    cur.execute(f"""
        SELECT 
            COUNT(DISTINCT o.id) as total_orders,
            COUNT(j.id) as total_jobs,
            SUM(i.quantity_ordered) as total_items
        FROM jobs j
        JOIN orders o ON j.order_id = o.id
        LEFT JOIN items i ON i.job_id = j.id
        {where_clause}
    """)
    metrics = cur.fetchone()
    if not metrics:
        metrics = {'total_orders': 0, 'total_jobs': 0, 'total_items': 0}
    else:
        # Ensure we return 0 instead of None if sum is null
        metrics = dict(metrics)
        if metrics['total_items'] is None: metrics['total_items'] = 0

    # --- JOB LIST ---
    cur.execute(f"""
        SELECT 
            j.id, j.job_ticket_number, j.production_status,
            o.order_number, o.order_date, o.ship_to_company, o.city, o.state,
            o.address_validation_status
        FROM jobs j
        JOIN orders o ON j.order_id = o.id
        {where_clause}
        ORDER BY o.order_date ASC
        LIMIT %s OFFSET %s
    """, (per_page, offset))
    jobs_raw = cur.fetchall()
    
    # Enrich with items
    jobs = []
    for j in jobs_raw:
        job = dict(j)
        cur.execute("SELECT quantity_ordered, product_name FROM items WHERE job_id = %s", (job['id'],))
        job['line_items'] = cur.fetchall()
        jobs.append(job)
        
    # Total Count for Pagination
    cur.execute(f"""
        SELECT COUNT(*) as count 
        FROM jobs j
        JOIN orders o ON j.order_id = o.id
        {where_clause}
    """)
    total_jobs_count = cur.fetchone()['count']
    total_pages = math.ceil(total_jobs_count / per_page)
    
    pagination = {
        'page': page,
        'per_page': per_page,
        'total': total_jobs_count,
        'total_pages': total_pages
    }

    cur.close()
    conn.close()
    
    return render_template('production_review.html', 
                            jobs=jobs, 
                            metrics=metrics, 
                            pagination=pagination)

@app.route('/production/submit', methods=['POST'])
def submit_production():
    import subprocess
    import datetime
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    batch_id = f"BATCH_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        # Update Status NEW -> READY
        # Only for VALID orders
        cur.execute("""
            UPDATE jobs
            SET production_status = 'READY',
                production_batch_id = %s
            WHERE id IN (
                SELECT j.id FROM jobs j
                JOIN orders o ON j.order_id = o.id
                WHERE j.production_status = 'NEW'
                AND o.address_validation_status IN ('VALID', 'CORRECTED', 'CORRECTED_BY_BOOK', 'MANUALLY_CORRECTED')
            )
        """, (batch_id,))
        
        count = cur.rowcount
        conn.commit()
        
        if count > 0:
            flash(f"Submitted {count} jobs to production. Batch: {batch_id}", "success")
            
            # Trigger Pipeline
            # Note: We assume run_pipeline.command or python pipeline/00_Controller.py
            # Using absolute path for safety
            pipeline_script = os.path.join(project_root, 'pipeline', '00_Controller.py')
            
            # Determine python interpreter (venv)
            python_exe = sys.executable 
            # If we are running in venv, sys.executable is the venv python.
            
            # Using Popen to run in background
            subprocess.Popen([python_exe, pipeline_script])
            flash("Production Pipeline triggered in background!", "info")
            
        else:
            flash("No valid jobs found to submit.", "warning")
            
    except Exception as e:
        conn.rollback()
        flash(f"Error submitting to production: {e}", "danger")
        print(f"Submit Error: {e}")
    finally:
        cur.close()
        conn.close()
        
    return redirect(url_for('dashboard'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False, port=5002)
