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
    
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    page = request.args.get('page', 1, type=int)
    # If a date filter is applied, show all results on one page for easy submission
    per_page = 10000 if (start_date or end_date) else 250
    offset = (page - 1) * per_page
    


    where_clause = """
        WHERE j.production_status = 'NEW'
          AND o.address_validation_status IN ('VALID', 'AUTO_CORRECTED', 'MANUALLY_CORRECTED', 'ADDRESS_BOOK_VERIFIED')
    """
    
    query_params = []
    date_where = ""
    date_params = []
    
    if start_date:
        where_clause += " AND DATE(o.order_date) >= %s"
        query_params.append(start_date)
        date_where += " AND DATE(order_date) >= %s"
        date_params.append(start_date)
    if end_date:
        where_clause += " AND DATE(o.order_date) <= %s"
        query_params.append(end_date)
        date_where += " AND DATE(order_date) <= %s"
        date_params.append(end_date)
    
    # Metrics query (unchanged)
    cur.execute(f"""
        SELECT 
            COUNT(DISTINCT o.id) as total_orders,
            COUNT(DISTINCT j.id) as total_jobs,
            COUNT(i.id) as total_line_items
        FROM jobs j
        JOIN orders o ON j.order_id = o.id
        LEFT JOIN items i ON i.job_id = j.id
        {where_clause}
    """, tuple(query_params))
    metrics = cur.fetchone()
    if not metrics:
        metrics = {'total_orders': 0, 'total_jobs': 0, 'total_line_items': 0}
    else:
        metrics = dict(metrics)

    # Fetch extra stats for the dynamic UI
    # 1. Pending Address Validation
    cur.execute(f"SELECT COUNT(*) as count FROM orders WHERE address_validation_status = 'PENDING'{date_where}", tuple(date_params))
    pending_count = cur.fetchone()['count']
    
    # 2. Address Exceptions (INVALID, AMBIGUOUS, EXCEPTION)
    cur.execute(f"SELECT COUNT(*) as count FROM orders WHERE address_validation_status IN ('EXCEPTION', 'AMBIGUOUS', 'INVALID'){date_where}", tuple(date_params))
    exception_count = cur.fetchone()['count']
    
    metrics['pending_count'] = pending_count
    metrics['exception_count'] = exception_count

    # Step 1: Get paginated job IDs (1 query)
    id_query_params = query_params + [per_page, offset]
    cur.execute(f"""
        SELECT j.id
        FROM jobs j
        JOIN orders o ON j.order_id = o.id
        {where_clause}
        ORDER BY o.order_date ASC, o.order_number ASC
        LIMIT %s OFFSET %s
    """, tuple(id_query_params))
    job_ids = [row['id'] for row in cur.fetchall()]

    # Step 2: Fetch all jobs + items in ONE query (eliminates N+1)
    jobs = []
    if job_ids:
        cur.execute("""
            SELECT 
                j.id, j.job_ticket_number, j.production_status,
                o.order_number, o.order_date, o.ship_to_company, o.city, o.state,
                o.address_validation_status, o.store_number,
                i.quantity_ordered, i.product_name, i.sku, i.job_ticket_display_id
            FROM jobs j
            JOIN orders o ON j.order_id = o.id
            LEFT JOIN items i ON i.job_id = j.id
            WHERE j.id = ANY(%s)
            ORDER BY o.order_date ASC, o.order_number ASC, i.job_ticket_display_id ASC
        """, (job_ids,))
        rows = cur.fetchall()

        # Group rows into jobs with nested line_items
        from collections import OrderedDict
        jobs_dict = OrderedDict()
        for row in rows:
            row = dict(row)
            job_id = row['id']
            if job_id not in jobs_dict:
                jobs_dict[job_id] = {
                    'id': row['id'],
                    'job_ticket_number': row['job_ticket_number'],
                    'production_status': row['production_status'],
                    'order_number': row['order_number'],
                    'order_date': row['order_date'],
                    'ship_to_company': row['ship_to_company'],
                    'city': row['city'],
                    'state': row['state'],
                    'address_validation_status': row['address_validation_status'],
                    'store_number': row['store_number'],
                    'line_items': []
                }
            if row['job_ticket_display_id'] is not None:
                jobs_dict[job_id]['line_items'].append({
                    'quantity_ordered': row['quantity_ordered'],
                    'product_name': row['product_name'],
                    'sku': row['sku'],
                    'job_ticket_display_id': row['job_ticket_display_id']
                })
        jobs = list(jobs_dict.values())

    # Total count for pagination
    cur.execute(f"""
        SELECT COUNT(*) as count 
        FROM jobs j
        JOIN orders o ON j.order_id = o.id
        {where_clause}
    """, tuple(query_params))
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
                            pagination=pagination,
                            start_date=start_date,
                            end_date=end_date)

@production_bp.route('/production/submit', methods=['POST'])
def submit_production():
    import subprocess
    import datetime
    
    conn = get_db()
    cur = conn.cursor()
    
    start_date = request.values.get('start_date')
    end_date = request.values.get('end_date')
    
    if start_date and end_date:
        if start_date == end_date:
            timestamp = start_date
        else:
            timestamp = f"{start_date}_to_{end_date}"
    elif start_date:
        timestamp = start_date
    elif end_date:
        timestamp = end_date
    else:
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        
    batch_id = f"BATCH_{timestamp}"
    dry_run = request.values.get('dry_run') == 'true'
    
    where_clause = "WHERE j.production_status = 'NEW' AND o.address_validation_status IN ('VALID', 'AUTO_CORRECTED', 'MANUALLY_CORRECTED', 'ADDRESS_BOOK_VERIFIED')"
    query_params = []
    
    if start_date:
        where_clause += " AND DATE(o.order_date) >= %s"
        query_params.append(start_date)
    if end_date:
        where_clause += " AND DATE(o.order_date) <= %s"
        query_params.append(end_date)
        
    # %s for SET comes first, then the subquery %s values.
    # So param list is [batch_id, start_date (optional), end_date (optional)]
    final_params = [batch_id] + query_params
    
    print(f"DEBUG flask submit: start_date={start_date}, end_date={end_date}, dry_run={dry_run}")
    
    try:
        if dry_run:
            cur.execute(f"""
                SELECT COUNT(*) FROM jobs j
                JOIN orders o ON j.order_id = o.id
                {where_clause}
            """, tuple(query_params))
            count = cur.fetchone()[0]
        else:
            cur.execute(f"""
                UPDATE jobs
                SET production_status = 'READY',
                    production_batch_id = %s
                WHERE id IN (
                    SELECT j.id FROM jobs j
                    JOIN orders o ON j.order_id = o.id
                    {where_clause}
                )
            """, tuple(final_params))
            count = cur.rowcount
            conn.commit()
        
        if count > 0:
            if dry_run:
                flash(f"[DRY RUN] Simulating pipeline for {count} jobs. Batch: {batch_id}", "info")
            else:
                flash(f"Submitted {count} jobs to production. Batch: {batch_id}", "success")
            
            pipeline_script = os.path.join(project_root, 'pipeline', '00_Controller.py')
            python_exe = sys.executable 
            
            cmd = [python_exe, pipeline_script]
            if start_date:
                cmd.extend(['--start_date', str(start_date)])
            if end_date:
                cmd.extend(['--end_date', str(end_date)])
            if dry_run:
                cmd.append('--dry_run')
            
            subprocess.Popen(cmd, cwd=project_root)
            if dry_run:
                flash("Production Pipeline triggered in DRY RUN mode!", "warning")
            else:
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
