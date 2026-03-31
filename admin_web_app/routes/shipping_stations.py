from flask import Blueprint, render_template, request, flash, redirect, url_for
from shared_lib.database import get_db_connection

shipping_stations_bp = Blueprint('shipping_stations', __name__, url_prefix='/shipping-stations')

@shipping_stations_bp.route('/')
def index():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, station_id, display_name, smb_path, is_active FROM shipping_stations ORDER BY display_name;")
    stations = []
    for row in cur.fetchall():
        stations.append({
            'id': row[0],
            'station_id': row[1],
            'display_name': row[2],
            'smb_path': row[3],
            'is_active': row[4]
        })
    cur.close()
    conn.close()
    return render_template('shipping_stations.html', stations=stations)

@shipping_stations_bp.route('/add', methods=['POST'])
def add_station():
    station_id = request.form.get('station_id', '').strip()
    display_name = request.form.get('display_name', '').strip()
    smb_path = request.form.get('smb_path', '').strip()
    is_active = request.form.get('is_active') == 'on'

    if not station_id or not display_name or not smb_path:
        flash("Station ID, Display Name, and SMB Path are requested.", "error")
        return redirect(url_for('shipping_stations.index'))

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO shipping_stations (station_id, display_name, smb_path, is_active)
            VALUES (%s, %s, %s, %s)
        """, (station_id, display_name, smb_path, is_active))
        conn.commit()
        flash(f"Station {display_name} added successfully.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Error adding station: {e}", "error")
    finally:
        cur.close()
        conn.close()
    
    return redirect(url_for('shipping_stations.index'))

@shipping_stations_bp.route('/edit/<int:id>', methods=['POST'])
def edit_station(id):
    station_id = request.form.get('station_id', '').strip()
    display_name = request.form.get('display_name', '').strip()
    smb_path = request.form.get('smb_path', '').strip()
    is_active = request.form.get('is_active') == 'on'

    if not station_id or not display_name or not smb_path:
        flash("Station ID, Display Name, and SMB Path are required.", "error")
        return redirect(url_for('shipping_stations.index'))

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE shipping_stations 
            SET station_id = %s, display_name = %s, smb_path = %s, is_active = %s
            WHERE id = %s
        """, (station_id, display_name, smb_path, is_active, id))
        conn.commit()
        flash(f"Station {display_name} updated successfully.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Error updating station: {e}", "error")
    finally:
        cur.close()
        conn.close()
    
    return redirect(url_for('shipping_stations.index'))

@shipping_stations_bp.route('/delete/<int:id>', methods=['POST'])
def delete_station(id):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM shipping_stations WHERE id = %s", (id,))
        conn.commit()
        flash("Station deleted successfully.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Error deleting station: {e}", "error")
    finally:
        cur.close()
        conn.close()
    
    return redirect(url_for('shipping_stations.index'))
