
import psycopg2
import sys

import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

DB_NAME = os.getenv("DB_NAME", "marcom_production_suite")
DB_USER = os.getenv("DB_USER", "jimmyswindler")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def create_table():
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor()
        
        print("Creating shipments table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shipments (
                id SERIAL PRIMARY KEY,
                order_number VARCHAR(50),
                tracking_number VARCHAR(100),
                marcom_sync_status VARCHAR(50) DEFAULT 'PENDING',
                marcom_response_message TEXT,
                reference_id VARCHAR(100),
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        print("Table 'shipments' created successfully.")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    create_table()
