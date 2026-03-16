import re
from .config import load_yaml_config

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

def get_product_category(product_id):
    """
    Maps a static product_id string (e.g. '218') to its config category (e.g. '12ptBounceBack').
    Returns None if not found.
    """
    if not product_id: return None
    config = load_yaml_config()
    product_ids_map = config.get("product_ids", {})
    
    prod_str = str(product_id).strip()
        
    for category, ids in product_ids_map.items():
        if prod_str in [str(i) for i in ids]:
            return category
            
    return None
