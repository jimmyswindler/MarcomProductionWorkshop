from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from shared_lib.database import get_db_connection

cartons_bp = Blueprint('cartons', __name__, url_prefix='/shipping-cartons')

@cartons_bp.route('/')
def index():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT code, name, weight, length, width, height FROM shipping_cartons ORDER BY code;")
    cartons = []
    for row in cur.fetchall():
        cartons.append({
            'code': row[0],
            'name': row[1],
            'weight': float(row[2]) if row[2] else 0.0,
            'length': row[3],
            'width': row[4],
            'height': row[5]
        })
    cur.close()
    conn.close()
    return render_template('cartons.html', cartons=cartons)

@cartons_bp.route('/add', methods=['POST'])
def add_carton():
    code = request.form.get('code', '').strip().upper()
    name = request.form.get('name', '').strip()
    weight = request.form.get('weight', 0.0)
    length = request.form.get('length', 0)
    width = request.form.get('width', 0)
    height = request.form.get('height', 0)

    if not code or not name:
        flash("Code and Name are required.", "error")
        return redirect(url_for('cartons.index'))

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO shipping_cartons (code, name, weight, length, width, height)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (code, name, weight, length, width, height))
        conn.commit()
        flash(f"Carton {code} added successfully.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Error adding carton: {e}", "error")
    finally:
        cur.close()
        conn.close()
    
    return redirect(url_for('cartons.index'))


@cartons_bp.route('/edit/<code>', methods=['POST'])
def edit_carton(code):
    new_code = request.form.get('code', '').strip().upper()
    name = request.form.get('name', '').strip()
    weight = request.form.get('weight', 0.0)
    length = request.form.get('length', 0)
    width = request.form.get('width', 0)
    height = request.form.get('height', 0)

    if not new_code or not name:
        flash("Code and Name are required.", "error")
        return redirect(url_for('cartons.index'))

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE shipping_cartons 
            SET code = %s, name = %s, weight = %s, length = %s, width = %s, height = %s
            WHERE code = %s
        """, (new_code, name, weight, length, width, height, code))
        conn.commit()
        flash(f"Carton {new_code} updated successfully.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Error updating carton: {e}", "error")
    finally:
        cur.close()
        conn.close()
    
    return redirect(url_for('cartons.index'))

@cartons_bp.route('/delete/<code>', methods=['POST'])
def delete_carton(code):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM shipping_cartons WHERE code = %s", (code,))
        conn.commit()
        flash(f"Carton {code} deleted successfully.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Error deleting carton: {e}", "error")
    finally:
        cur.close()
        conn.close()
    
    return redirect(url_for('cartons.index'))
