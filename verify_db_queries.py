
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv
import time

project_root = os.getcwd()
load_dotenv(os.path.join(project_root, '.env'))

DB_NAME = os.getenv("DB_NAME", "marcom_production_suite")
DB_USER = os.getenv("DB_USER", "jimmyswindler")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def check_queries():
    try:
        print("Connecting...")
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        print("Query 1: Address Book Count")
        start = time.time()
        cur.execute("SELECT COUNT(*) FROM address_book")
        res = cur.fetchone()
        print(f"  Result: {res['count']} (Took {time.time()-start:.4f}s)")
        
        print("Query 2: Exceptions Count")
        start = time.time()
        cur.execute("SELECT COUNT(*) FROM orders WHERE address_validation_status IN ('AMBIGUOUS', 'INVALID')")
        res = cur.fetchone()
        print(f"  Result: {res['count']} (Took {time.time()-start:.4f}s)")
        
        cur.close()
        conn.close()
        print("Done.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_queries()
