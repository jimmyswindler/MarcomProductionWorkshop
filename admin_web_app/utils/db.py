import os
import sys

# Add project root to path so we can import from shared_lib
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

from shared_lib.database import get_db_connection, get_real_dict_cursor

def get_db():
    return get_db_connection()
