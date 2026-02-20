import sys
import os
sys.path.append(os.getcwd())
from shared_lib.database import get_db_connection, get_real_dict_cursor

conn = get_db_connection()
cur = get_real_dict_cursor(conn)
cur.execute("SELECT current_database()")
print(cur.fetchone())

cur.execute("SELECT order_number FROM orders LIMIT 5")
print(cur.fetchall())
