
import psycopg2
import os
from dotenv import load_dotenv

project_root = os.getcwd()
load_dotenv(os.path.join(project_root, '.env'))

DB_NAME = os.getenv("DB_NAME", "marcom_production_suite")
DB_USER = os.getenv("DB_USER", "jimmyswindler")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def check_locks():
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor()
        
        print("--- Checking Locks ---")
        cur.execute("""
            SELECT 
                pg_class.relname, 
                pg_locks.transactionid, 
                pg_locks.mode, 
                pg_locks.granted,
                pg_stat_activity.query AS query_snippet
            FROM pg_locks 
            JOIN pg_class ON pg_locks.relation = pg_class.oid
            JOIN pg_stat_activity ON pg_locks.pid = pg_stat_activity.pid
            WHERE pg_class.relname IN ('orders', 'shipments', 'jobs', 'item_boxes')
            AND pg_stat_activity.pid <> pg_backend_pid();
        """)
        rows = cur.fetchall()
        if not rows:
            print("No locks found on key tables.")
        else:
            for row in rows:
                print(f"Lock: Table={row[0]}, Mode={row[2]}, Granted={row[3]}, Query={row[4][:50]}...")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_locks()
