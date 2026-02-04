
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

DB_NAME = os.getenv("DB_NAME", "marcom_production_suite")
DB_USER = os.getenv("DB_USER", "jimmyswindler")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def get_schema():
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor()
        
        # Get all tables
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cur.fetchall()
        
        print(f"DATABASE: {DB_NAME}")
        print("="*30)
        
        for table in tables:
            t_name = table[0]
            print(f"\nTABLE: {t_name}")
            print("-" * (len(t_name) + 7))
            
            # Get Columns
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position;
            """, (t_name,))
            columns = cur.fetchall()
            
            for col in columns:
                print(f"  - {col[0]} ({col[1]}) {'NULL' if col[2]=='YES' else 'NOT NULL'}")

            # Get Foreign Keys
            cur.execute("""
                SELECT
                    kcu.column_name, 
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name 
                FROM 
                    information_schema.key_column_usage AS kcu
                    JOIN information_schema.constraint_column_usage AS ccu
                    ON kcu.constraint_name = ccu.constraint_name
                    JOIN information_schema.table_constraints AS tc
                    ON kcu.constraint_name = tc.constraint_name
                WHERE kcu.table_name = %s AND tc.constraint_type = 'FOREIGN KEY';
            """, (t_name,))
            fks = cur.fetchall()
            if fks:
                print("  Running Connections:")
                for fk in fks:
                    print(f"    * {fk[0]} -> {fk[1]}.{fk[2]}")

        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    get_schema()
