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
