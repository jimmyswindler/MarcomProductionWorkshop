import re

def get_store_number(name_string):
    """Extracts store number from a name string like 'Store #123'."""
    if not name_string: return "0000"
    match = re.search(r'#\s*(\d+)', str(name_string))
    if match:
        return match.group(1).zfill(4)
    return "0000"

def extract_store_number_strict(text):
    """
    Stricter extraction: Looks for 'Store' or '#' followed by digits.
    Returns the digits or None.
    """
    if not text: return None
    match = re.search(r'(?:store|#)\s*[\.\-]?\s*(\d+)', str(text), re.IGNORECASE)
    if match:
        return match.group(1)
    return None

from functools import lru_cache

@lru_cache(maxsize=1024)
def get_product_category(product_id):
    """
    Maps a static product_id string (e.g. '218') to its config category (e.g. '12ptBounceBack')
    using a direct Postgres lookup with LRU caching for performance.
    Returns None if not found.
    """
    if not product_id: return None
    
    from .database import get_db_connection
    prod_str = str(product_id).strip()
    
    conn = None
    try:
        conn = get_db_connection()
        if conn:
            cur = conn.cursor()
            cur.execute("SELECT category_name FROM app_products WHERE marcom_id = %s", (prod_str,))
            row = cur.fetchone()
            cur.close()
            if row: return row[0]
    except Exception as e:
        print(f"Error fetching product category for {prod_str}: {e}")
    finally:
        if conn: conn.close()
            
    return None
