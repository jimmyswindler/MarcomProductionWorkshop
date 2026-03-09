from flask import Blueprint, render_template, request, redirect, url_for, flash
import math
import os
import sys
from utils.db import get_db, get_real_dict_cursor

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

production_bp = Blueprint('production', __name__)

@production_bp.route('/production-review')
def production_review():
    conn = get_db()
    cur = get_real_dict_cursor(conn)
    
    page = request.args.get('page', 1, type=int)
    per_page = 20
    offset = (page - 1) * per_page

    where_clause = """
        WHERE j.production_status = 'NEW'
          AND o.address_validation_status IN ('VALID', 'AUTO_CORRECTED', 'MANUALLY_CORRECTED')
    """
    
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
        metrics = dict(metrics)
        if metrics['total_items'] is None: metrics['total_items'] = 0

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
    
    jobs = []
    for j in jobs_raw:
        job = dict(j)
        cur.execute("SELECT quantity_ordered, product_name FROM items WHERE job_id = %s", (job['id'],))
        job['line_items'] = cur.fetchall()
        jobs.append(job)
        
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

@production_bp.route('/production/submit', methods=['POST'])
def submit_production():
    import subprocess
    import datetime
    
    conn = get_db()
    cur = conn.cursor()
    
    batch_id = f"BATCH_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        cur.execute("""
            UPDATE jobs
            SET production_status = 'READY',
                production_batch_id = %s
            WHERE id IN (
                SELECT j.id FROM jobs j
                JOIN orders o ON j.order_id = o.id
                WHERE j.production_status = 'NEW'
                AND o.address_validation_status IN ('VALID', 'AUTO_CORRECTED', 'MANUALLY_CORRECTED')
            )
        """, (batch_id,))
        
        count = cur.rowcount
        conn.commit()
        
        if count > 0:
            flash(f"Submitted {count} jobs to production. Batch: {batch_id}", "success")
            
            pipeline_script = os.path.join(project_root, 'pipeline', '00_Controller.py')
            python_exe = sys.executable 
            
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
        
    return redirect(url_for('dashboard.dashboard'))
