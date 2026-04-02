from flask import Blueprint, render_template, request, redirect, url_for, flash
from utils.db import get_db, get_real_dict_cursor
import json

settings_bp = Blueprint('settings', __name__, url_prefix='/settings')

@settings_bp.route('/')
def index():
    conn = get_db()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('dashboard.index'))
        
    cur = get_real_dict_cursor(conn)
    cur.execute("SELECT key, value, description FROM global_settings ORDER BY key ASC")
    raw_settings = cur.fetchall()
    
    # Pre-format JSON for display in textareas
    settings = []
    for s in raw_settings:
        try:
            # indent=4 makes nice readable multi-line JSON
            s['formatted_value'] = json.dumps(s['value'], indent=4)
        except:
            s['formatted_value'] = str(s['value'])
        settings.append(s)
    
    cur.close()
    conn.close()
    return render_template('settings.html', settings=settings)

@settings_bp.route('/update', methods=['POST'])
def update_setting():
    key = request.form.get('key')
    value_str = request.form.get('value')
    
    if not key or not value_str:
        flash('Missing key or value.', 'error')
        return redirect(url_for('settings.index'))
        
    conn = get_db()
    cur = conn.cursor()
    
    try:
        # Validate that the string is parseable JSON before saving
        parsed_json = json.loads(value_str)
        # Dump to ensure correct quote formatting
        valid_json_str = json.dumps(parsed_json)
        
        cur.execute("""
            UPDATE global_settings 
            SET value = %s::jsonb 
            WHERE key = %s
        """, (valid_json_str, key))
        conn.commit()
        flash(f"Global configuration '{key}' updated successfully.", 'success')
    except json.JSONDecodeError:
        flash(f"Invalid JSON structure for '{key}'. Ensure your syntax is correct.", 'error')
    except Exception as e:
        conn.rollback()
        flash(f"Database error updating '{key}': {str(e)}", 'error')
    finally:
        cur.close()
        conn.close()
        
    return redirect(url_for('settings.index'))
