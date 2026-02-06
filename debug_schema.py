
import psycopg2
import os
from dotenv import load_dotenv

project_root = os.getcwd()
load_dotenv(os.path.join(project_root, '.env'))

DB_NAME = os.getenv("DB_NAME", "marcom_production_suite")
DB_USER = os.getenv("DB_USER", "jimmyswindler")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def check_schema():
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor()
        
        tables = ['orders', 'shipments', 'jobs']
        for t in tables:
            print(f"--- Table: {t} ---")
            cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{t}'")
            for row in cur.fetchall():
                print(f"  {row[0]} ({row[1]})")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_schema()
