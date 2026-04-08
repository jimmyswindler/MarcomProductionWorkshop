from flask import Blueprint, render_template, request, redirect, url_for, flash
from utils.db import get_db, get_real_dict_cursor
import pandas as pd
import tempfile
import json
import psycopg2.extras

address_book_bp = Blueprint('address_book', __name__)

@address_book_bp.route('/address-book')
def address_book():
    search = request.args.get('search', '')
    conn = get_db()
    cur = get_real_dict_cursor(conn)
    
    if search:
        cur.execute("SELECT * FROM address_book WHERE is_active = TRUE AND (store_number ILIKE %s OR company_name ILIKE %s) ORDER BY store_number", (f'%{search}%', f'%{search}%'))
    else:
        cur.execute("SELECT * FROM address_book WHERE is_active = TRUE ORDER BY store_number") 
    
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
                company_name = %s, attn = %s, store_name = %s, ups_days = %s,
                address1 = %s, address2 = %s, address3 = %s,
                city = %s, state = %s, zip = %s,
                last_updated = NOW()
            WHERE store_number = %s
        """
        ups_d = request.form.get('ups_days')
        ups_d = int(ups_d) if ups_d and ups_d.isdigit() else None
        
        cur.execute(sql, (
            request.form['company_name'], request.form['attn'], request.form.get('store_name', ''), ups_d,
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

@address_book_bp.route('/address-book/upload', methods=['POST'])
def upload_addresses():
    if 'address_file' not in request.files:
        flash('No file part', 'error')
        return redirect(url_for('address_book.address_book'))
    file = request.files['address_file']
    if file.filename == '':
        flash('No selected file', 'error')
        return redirect(url_for('address_book.address_book'))

    try:
        df = pd.read_excel(file)
        
        # Sniff format
        is_vendor = 'Store ID' in df.columns
        is_worldship = 'NUMBER' in df.columns
        
        if not is_vendor and not is_worldship:
            flash("Error parsing Excel: Unrecognized file format (Missing 'Store ID' or 'NUMBER')", 'error')
            return redirect(url_for('address_book.address_book'))
            
    except Exception as e:
        flash(f"Error parsing Excel: {e}", 'error')
        return redirect(url_for('address_book.address_book'))

    # Load current db state
    conn = get_db()
    cur = get_real_dict_cursor(conn)
    cur.execute("SELECT * FROM address_book")
    current_records = cur.fetchall()
    cur.close()
    conn.close()

    db_map = {str(r['store_number']).strip(): r for r in current_records}
    excel_map = {}

    for _, row in df.iterrows():
        if is_vendor:
            st_num = str(row['Store ID']).replace('.0', '').strip().zfill(4)
            if not st_num or st_num.lower() == 'nan' or st_num == '0000': continue
            
            excel_map[st_num] = {
                'store_number': st_num,
                'store_name': str(row.get('Name', '')).strip() if pd.notna(row.get('Name')) else '',
                'address1': str(row.get('Address', '')).strip() if pd.notna(row.get('Address')) else '',
                'city': str(row.get('City', '')).strip() if pd.notna(row.get('City')) else '',
                'state': str(row.get('State', '')).strip() if pd.notna(row.get('State')) else '',
                'zip': str(row.get('Zip', ''))[:5].strip() if pd.notna(row.get('Zip')) else '',
                'phone': str(row.get('Phone', '')).strip() if pd.notna(row.get('Phone')) else '',
                'email': str(row.get('Email', '')).strip() if pd.notna(row.get('Email')) else '',
            }
        else:
            st_num = str(row['NUMBER']).replace('.0', '').strip().zfill(4)
            if not st_num or st_num.lower() == 'nan' or st_num == '0000': continue
            
            ups_d = None
            if pd.notna(row.get('UPS Days')):
                try: ups_d = int(float(row['UPS Days']))
                except: pass
                
            excel_map[st_num] = {
                'store_number': st_num,
                'company_name': str(row.get('COMPANY', '')).strip() if pd.notna(row.get('COMPANY')) else '',
                'store_name': str(row.get('Name', '')).strip() if pd.notna(row.get('Name')) else '',
                'address1': str(row.get('Address1', '')).strip() if pd.notna(row.get('Address1')) else '',
                'address2': str(row.get('ADD2', '')).strip() if pd.notna(row.get('ADD2')) else '',
                'address3': str(row.get('ADD3', '')).strip() if pd.notna(row.get('ADD3')) else '',
                'attn': str(row.get('ATTN', '')).strip() if pd.notna(row.get('ATTN')) else '',
                'city': str(row.get('City', '')).strip() if pd.notna(row.get('City')) else '',
                'state': str(row.get('State', '')).strip() if pd.notna(row.get('State')) else '',
                'zip': str(row.get('Zip', ''))[:5].strip() if pd.notna(row.get('Zip')) else '',
                'phone': str(row.get('Phone', '')).strip() if pd.notna(row.get('Phone')) else '',
                'email': str(row.get('Email', '')).strip() if pd.notna(row.get('Email')) else '',
                'ups_days': ups_d
            }

    new_entries = []
    modified_entries = []
    active_in_excel = set()

    for st_num, ex_data in excel_map.items():
        active_in_excel.add(st_num)
        db_data = db_map.get(st_num)
        
        if not db_data:
            # Base payload for insert, padding missing keys
            payload = ex_data.copy()
            if 'company_name' not in payload: payload['company_name'] = ''
            if 'address2' not in payload: payload['address2'] = ''
            if 'address3' not in payload: payload['address3'] = ''
            if 'attn' not in payload: payload['attn'] = ''
            if 'ups_days' not in payload: payload['ups_days'] = None
            new_entries.append(payload)
        else:
            # Check for modifications
            mods = {}
            payload = db_data.copy() # We bundle all existing data to prevent blanking unprovided fields
            
            for k in ex_data.keys():
                if k == 'store_number': continue
                
                ex_val = ex_data.get(k)
                db_val = db_data.get(k)
                
                if k == 'ups_days':
                    if ex_val is not None and db_val != ex_val:
                        mods[k] = {'old': db_val, 'new': ex_val}
                        payload[k] = ex_val
                else:
                    db_val_str = '' if db_val is None else str(db_val).strip()
                    if db_val_str != ex_val:
                        mods[k] = {'old': db_val_str, 'new': ex_val}
                        payload[k] = ex_val
            
            if mods or db_data.get('is_active') is False:
                payload['changes_summary'] = mods
                payload['reactivated'] = db_data.get('is_active') is False
                
                # Coalesce datetime objects out so json.dump doesn't break
                if 'last_updated' in payload: del payload['last_updated']
                
                modified_entries.append(payload)

    removed_entries = []
    for st_num, db_data in db_map.items():
        if st_num not in active_in_excel and db_data.get('is_active', True):
            # Coalesce datetime objects out so json.dump doesn't break
            payload = db_data.copy()
            if 'last_updated' in payload: del payload['last_updated']
            removed_entries.append(payload)

    payload = {
        'new_entries': new_entries,
        'modified_entries': modified_entries,
        'removed_entries': removed_entries,
        'source_format': 'Vendor List' if is_vendor else 'UPS Worldship'
    }

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
    with open(tmp.name, 'w') as f:
        json.dump(payload, f)

    return render_template('address_book_preview.html', payload=payload, tmp_file=tmp.name)

@address_book_bp.route('/address-book/sync', methods=['POST'])
def sync_addresses():
    tmp_file = request.form.get('tmp_file')
    if not tmp_file:
        flash("Session expired or invalid payload", "error")
        return redirect(url_for('address_book.address_book'))
    
    try:
        with open(tmp_file, 'r') as f:
            payload = json.load(f)
    except Exception as e:
        flash(f"Error reading sync payload: {e}", "error")
        return redirect(url_for('address_book.address_book'))

    new_entries = payload.get('new_entries', [])
    modified_entries = payload.get('modified_entries', [])
    removed_entries = payload.get('removed_entries', [])

    conn = get_db()
    cur = conn.cursor()

    try:
        if new_entries:
            insert_sql = """
                INSERT INTO address_book
                (store_number, store_name, company_name, address1, address2, address3, attn, city, state, zip, phone, email, ups_days, is_active, last_updated)
                VALUES %s
            """
            insert_vals = [
                (e['store_number'], e['store_name'], e['company_name'], e['address1'], e['address2'], e['address3'], e['attn'], e['city'], e['state'], e['zip'], e['phone'], e['email'], e['ups_days'], True, 'NOW()')
                for e in new_entries
            ]
            psycopg2.extras.execute_values(cur, insert_sql, insert_vals)

        if modified_entries:
            update_sql = """
                UPDATE address_book SET
                    store_name = data.store_name,
                    company_name = data.company_name,
                    address1 = data.address1,
                    address2 = data.address2,
                    address3 = data.address3,
                    attn = data.attn,
                    city = data.city,
                    state = data.state,
                    zip = data.zip,
                    phone = data.phone,
                    email = data.email,
                    ups_days = data.ups_days::integer,
                    is_active = TRUE,
                    last_updated = NOW()
                FROM (VALUES %s) as data(store_number, store_name, company_name, address1, address2, address3, attn, city, state, zip, phone, email, ups_days)
                WHERE address_book.store_number = data.store_number
            """
            update_vals = [
                (e['store_number'], e.get('store_name',''), e.get('company_name',''), e.get('address1',''), e.get('address2',''), e.get('address3',''), e.get('attn',''), e.get('city',''), e.get('state',''), e.get('zip',''), e.get('phone',''), e.get('email',''), e.get('ups_days'))
                for e in modified_entries
            ]
            psycopg2.extras.execute_values(cur, update_sql, update_vals)

        if removed_entries:
            remove_sql = "UPDATE address_book SET is_active = FALSE, last_updated = NOW() WHERE store_number IN %s"
            rem_ids = tuple([e['store_number'] for e in removed_entries])
            if len(rem_ids) == 1:
                cur.execute("UPDATE address_book SET is_active = FALSE, last_updated = NOW() WHERE store_number = %s", (rem_ids[0],))
            else:
                cur.execute(remove_sql, (rem_ids,))

        conn.commit()
        flash(f"Sync complete ({payload.get('source_format')}): {len(new_entries)} added, {len(modified_entries)} updated, {len(removed_entries)} removed.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Database error during sync: {e}", "error")
    finally:
        cur.close()
        conn.close()

    return redirect(url_for('address_book.address_book'))
