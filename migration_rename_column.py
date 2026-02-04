
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

DB_NAME = os.getenv("DB_NAME", "marcom_production_suite")
DB_USER = os.getenv("DB_USER", "jimmyswindler")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def run_migration():
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor()
        
        print("Renaming column 'job_ticket_number' to 'order_number' in 'shipments' table...")
        
        # Check if column exists first to avoid error if re-run
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='shipments' AND column_name='job_ticket_number';
        """)
        if cur.fetchone():
            cur.execute("ALTER TABLE shipments RENAME COLUMN job_ticket_number TO order_number;")
            conn.commit()
            print("SUCCESS: Column renamed.")
        else:
            print("SKIPPED: Column 'job_ticket_number' not found (already renamed?).")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Migration Failed: {e}")

if __name__ == "__main__":
    run_migration()
