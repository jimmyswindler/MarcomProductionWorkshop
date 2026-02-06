from flask import Flask, render_template, request, redirect, url_for, flash
import psycopg2
import psycopg2.extras
import os
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
    
    # --- PROPOSED CHART DATA (Last 30 Days) ---
    cur.execute("""
        WITH dates AS (
            SELECT generate_series(
                CURRENT_DATE - INTERVAL '30 days',
                CURRENT_DATE,
                '1 day'::interval
            )::date AS day
        )
        SELECT 
            TO_CHAR(d.day, 'YYYY-MM-DD') as day_str,
            COUNT(DISTINCT o.id) as orders_count,
            COUNT(DISTINCT j.id) as jobs_count,
            COUNT(DISTINCT s.id) as shipments_count
        FROM dates d
        LEFT JOIN orders o ON DATE(o.order_date) = d.day
        LEFT JOIN jobs j ON j.order_id = o.id -- Jobs linked to orders on that day (approx)
        LEFT JOIN shipments s ON DATE(s.created_at) = d.day
        GROUP BY d.day
        ORDER BY d.day ASC
    """)
    timeline_data = cur.fetchall()
    
    # Format for Chart.js
    chart_labels = [row['day_str'] for row in timeline_data]
    chart_orders = [row['orders_count'] for row in timeline_data]
    chart_jobs = [row['jobs_count'] for row in timeline_data]
    chart_shipments = [row['shipments_count'] for row in timeline_data]
    
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
    # Parse JSON if needed (psycopg2 RealDict might return dict for JSONB? Yes.)
    
    cur.close()
    conn.close()
    
    return render_template('dashboard.html', 
        total_ab=total_ab, 
        exceptions=exceptions,
        chart_labels=chart_labels,
        chart_orders=chart_orders,
        chart_jobs=chart_jobs,
        chart_shipments=chart_shipments,
        val_stats=val_stats,
        recent_corrections=recent_corrections
    )

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False, port=5002)
