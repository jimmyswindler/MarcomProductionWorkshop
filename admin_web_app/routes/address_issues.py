from flask import Blueprint, render_template, request, redirect, url_for, flash
import json
import re
from utils.db import get_db, get_real_dict_cursor

address_issues_bp = Blueprint('address_issues', __name__)

@address_issues_bp.route('/exceptions')
def exceptions():
    conn = get_db()
    cur = get_real_dict_cursor(conn)
    
    cur.execute("""
        SELECT * FROM orders 
        WHERE address_validation_status IN ('EXCEPTION', 'AMBIGUOUS', 'INVALID')
        ORDER BY order_date DESC
    """)
    orders_raw = cur.fetchall()
    
    orders = []
    for o in orders_raw:
        order = dict(o)
        
        if order.get('store_number') and str(order['store_number']).isdigit():
            order['store_number'] = str(order['store_number']).zfill(4)
            
        display_details = ""
        details_val = order.get('address_validation_details')
        if details_val:
            try:
                if isinstance(details_val, str):
                    parsed_details = json.loads(details_val)
                else:
                    parsed_details = details_val
                
                if "raw_response" in parsed_details:
                    xav_resp = parsed_details.get("raw_response", {}).get("XAVResponse", {})
                    candidates = xav_resp.get("Candidate", [])
                    if isinstance(candidates, list):
                         cand_count = len(candidates)
                    elif isinstance(candidates, dict):
                         cand_count = 1
                    else:
                         cand_count = 0
                    
                    if order.get("address_validation_status") == "AMBIGUOUS":
                        display_details = f"AMBIGUOUS - {cand_count} candidates found"
                    elif order.get("address_validation_status") == "INVALID":
                        display_details = parsed_details.get("status", "INVALID")
                else:
                    display_details = parsed_details.get("msg", str(parsed_details))
            except Exception:
                display_details = str(details_val)
                
        if len(display_details) > 50:
             display_details = display_details[:47] + "..."
             
        order['display_details'] = display_details
        orders.append(order)

    cur.close()
    conn.close()
    return render_template('exceptions.html', orders=orders)

@address_issues_bp.route('/exceptions/fix/<order_number>', methods=['GET', 'POST'])
def fix_exception(order_number):
    conn = get_db()
    cur = get_real_dict_cursor(conn)
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'manual_fix':
            sql = """
                UPDATE orders SET
                    address1 = %s, address2 = %s, address3 = %s, city = %s, state = %s, zip = %s, country = %s,
                    address_validated = TRUE,
                    address_validation_status = 'MANUALLY_CORRECTED',
                    address_validation_details = %s
                WHERE order_number = %s
            """
            details = json.dumps({'msg': 'Manually corrected via Admin UI'})
            cur.execute(sql, (
                request.form['address1'], request.form['address2'], request.form.get('address3', ''),
                request.form['city'], request.form['state'], request.form['zip'], request.form.get('country', 'US'),
                details, order_number
            ))
            conn.commit()
            flash(f'Order {order_number} corrected manually.', 'success')
            
        elif action == 'apply_book':
            store_key = request.form.get('store_number', '').strip()
            if store_key.isdigit(): store_key = store_key.zfill(4)
            
            cur.execute("SELECT * FROM address_book WHERE store_number = %s", (store_key,))
            book_entry = cur.fetchone()
            
            if book_entry:
                sql = """
                    UPDATE orders SET
                        address1 = %s, address2 = %s, address3 = %s,
                        city = %s, state = %s, zip = %s, country = 'US',
                        address_validated = TRUE,
                        address_validation_status = 'MANUALLY_CORRECTED',
                        address_validation_details = %s
                    WHERE order_number = %s
                """
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
                return redirect(url_for('address_issues.fix_exception', order_number=order_number))

        cur.close()
        conn.close()
        return redirect(url_for('address_issues.exceptions'))

    cur.execute("SELECT * FROM orders WHERE order_number = %s", (order_number,))
    order = cur.fetchone()
    
    if order.get('store_number') and str(order['store_number']).isdigit():
         order = dict(order)
         order['store_number'] = str(order['store_number']).zfill(4)

    guessed_store = order.get('store_number')
    if guessed_store and str(guessed_store).isdigit():
        guessed_store = str(guessed_store).zfill(4)

    cur.close()
    conn.close()
    return render_template('fix_exception.html', order=order, guessed_store=guessed_store)

@address_issues_bp.route('/exceptions/bulk_fix', methods=['POST'])
def bulk_fix_exception():
    conn = get_db()
    cur = get_real_dict_cursor(conn)
    
    order_numbers = request.form.getlist('order_numbers')
    store_key = request.form.get('store_number', '').strip()
    
    if store_key.isdigit(): 
        store_key = store_key.zfill(4)
        
    if not order_numbers or not store_key:
        flash('Must select orders and provide a store number.', 'danger')
        return redirect(url_for('address_issues.exceptions'))

    cur.execute("SELECT * FROM address_book WHERE store_number = %s", (store_key,))
    book_entry = cur.fetchone()
    
    if not book_entry:
        flash(f'Store #{store_key} not found in Address Book.', 'danger')
        return redirect(url_for('address_issues.exceptions'))

    success_count = 0
    for order_number in order_numbers:
        # Get original order address
        cur.execute("SELECT address1, zip FROM orders WHERE order_number = %s", (order_number,))
        orig_order = cur.fetchone()
        
        sql = """
            UPDATE orders SET
                address1 = %s, address2 = %s, address3 = %s,
                city = %s, state = %s, zip = %s, country = 'US',
                address_validated = TRUE,
                address_validation_status = 'MANUALLY_CORRECTED',
                address_validation_details = %s,
                store_number = %s
            WHERE order_number = %s
        """
        details = json.dumps({'msg': 'Bulk Applied from Address Book via Admin UI', 'store_number': store_key})
        cur.execute(sql, (
            book_entry['address1'], book_entry['address2'], book_entry['address3'],
            book_entry['city'], book_entry['state'], book_entry['zip'],
            details, store_key, order_number
        ))
        
        success_count += 1

    conn.commit()
    cur.close()
    conn.close()
    
    flash(f'Successfully fixed {success_count} orders using Store #{store_key}.', 'success')
    return redirect(url_for('address_issues.exceptions'))

