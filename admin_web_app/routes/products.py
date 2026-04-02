from flask import Blueprint, render_template, request, redirect, url_for, flash
from utils.db import get_db, get_real_dict_cursor

products_bp = Blueprint('products', __name__, url_prefix='/products')

@products_bp.route('/')
def index():
    conn = get_db()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('dashboard.index'))
        
    cur = get_real_dict_cursor(conn)
    cur.execute("SELECT id, marcom_id, category_name, standing_file FROM app_products ORDER BY marcom_id ASC")
    products = cur.fetchall()
    
    cur.execute("SELECT name FROM production_categories ORDER BY name ASC")
    categories = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return render_template('products.html', products=products, categories=categories)

@products_bp.route('/add', methods=['POST'])
def add_product():
    marcom_id = request.form.get('marcom_id', '').strip()
    category_name = request.form.get('category_name', '').strip()
    standing_file = request.form.get('standing_file', '').strip()
    
    if not marcom_id:
        flash('Marcom ID is required.', 'error')
        return redirect(url_for('products.index'))
        
    conn = get_db()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            INSERT INTO app_products (marcom_id, category_name, standing_file)
            VALUES (%s, %s, %s)
        """, (marcom_id, category_name if category_name else None, standing_file if standing_file else None))
        conn.commit()
        flash('Product added successfully.', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error adding product: {str(e)}', 'error')
    finally:
        cur.close()
        conn.close()
        
    return redirect(url_for('products.index'))

@products_bp.route('/edit/<int:id>', methods=['POST'])
def edit_product(id):
    marcom_id = request.form.get('marcom_id', '').strip()
    category_name = request.form.get('category_name', '').strip()
    standing_file = request.form.get('standing_file', '').strip()
    
    if not marcom_id:
        flash('Marcom ID is required.', 'error')
        return redirect(url_for('products.index'))
        
    conn = get_db()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            UPDATE app_products 
            SET marcom_id = %s, category_name = %s, standing_file = %s
            WHERE id = %s
        """, (marcom_id, category_name if category_name else None, standing_file if standing_file else None, id))
        conn.commit()
        flash('Product updated successfully.', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error updating product: {str(e)}', 'error')
    finally:
        cur.close()
        conn.close()
        
    return redirect(url_for('products.index'))

@products_bp.route('/delete/<int:id>', methods=['POST'])
def delete_product(id):
    conn = get_db()
    cur = conn.cursor()
    
    try:
        cur.execute("DELETE FROM app_products WHERE id = %s", (id,))
        conn.commit()
        flash('Product deleted successfully.', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting product: {str(e)}', 'error')
    finally:
        cur.close()
        conn.close()
        
    return redirect(url_for('products.index'))
