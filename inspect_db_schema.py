
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from shared_lib.config import get_env_var

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=get_env_var('DB_NAME', 'marcom_production_suite'),
            user=get_env_var('DB_USER', 'jimmyswindler'),
            password=get_env_var('DB_PASS', None),
            host=get_env_var('DB_HOST', 'localhost'),
            port=get_env_var('DB_PORT', '5432')
        )
        return conn
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        return None

def inspect():
    conn = get_db_connection()
    if not conn: return

    cur = conn.cursor()
    
    print("--- Table: shipments Schema ---")
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'shipments';
    """)
    for row in cur.fetchall():
        print(row)

    print("\n--- Recent Shipments ---")
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM shipments ORDER BY created_at DESC LIMIT 5")
    for row in cur.fetchall():
        print(row)
    
    conn.close()

if __name__ == "__main__":
    inspect()
