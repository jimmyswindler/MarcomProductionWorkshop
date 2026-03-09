from flask import Blueprint, render_template, request, redirect, url_for, flash
from utils.db import get_db, get_real_dict_cursor

address_book_bp = Blueprint('address_book', __name__)

@address_book_bp.route('/address-book')
def address_book():
    search = request.args.get('search', '')
    conn = get_db()
    cur = get_real_dict_cursor(conn)
    
    if search:
        cur.execute("SELECT * FROM address_book WHERE store_number ILIKE %s OR company_name ILIKE %s ORDER BY store_number", (f'%{search}%', f'%{search}%'))
    else:
        cur.execute("SELECT * FROM address_book ORDER BY store_number") 
    
    addresses = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('address_book.html', addresses=addresses, search=search)

@address_book_bp.route('/address-book/edit/<store_number>', methods=['GET', 'POST'])
def edit_address(store_number):
    conn = get_db()
    cur = get_real_dict_cursor(conn)
    
    if request.method == 'POST':
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
        return redirect(url_for('address_book.address_book'))
        
    cur.execute("SELECT * FROM address_book WHERE store_number = %s", (store_number,))
    addr = cur.fetchone()
    cur.close()
    conn.close()
    return render_template('edit_address.html', addr=addr)
