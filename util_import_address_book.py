import pandas as pd
import psycopg2
import os
import sys
import yaml
from dotenv import load_dotenv

# Load env from root
project_root = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(project_root, '.env'))

DB_NAME = os.getenv("DB_NAME", "marcom_production_suite")
DB_USER = os.getenv("DB_USER", "jimmyswindler")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def connect_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, host=DB_HOST, port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"DB Connection Failed: {e}")
        return None

def create_address_book_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS address_book (
            store_number TEXT PRIMARY KEY,
            company_name TEXT,
            attn TEXT,
            address1 TEXT,
            address2 TEXT,
            address3 TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            phone TEXT,
            email TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    print("Checked/Created address_book table.")

def clean_val(val):
    if pd.isna(val) or val == 'nan' or str(val).strip() == '':
        return None
    return str(val).strip()

def import_excel(file_path):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    print(f"Reading {file_path}...")
    df = pd.read_excel(file_path, engine='openpyxl')
    
    # Expected columns: NUMBER, Name, COMPANY, ATTN, Address1, ADD2, ADD3, City, State, Zip, Phone, Email
    # Map to DB columns
    
    conn = connect_db()
    if not conn: return
    
    create_address_book_table(conn)
    cur = conn.cursor()
    
    count = 0
    updated = 0
    
    for idx, row in df.iterrows():
        raw_num = row.get('NUMBER')
        if pd.isna(raw_num) or str(raw_num).strip() == '': continue
        
        # Normalize Store Number: Handle float 237.0 -> 237 -> 0237
        s_num = str(raw_num).strip()
        if s_num.endswith('.0'):
            s_num = s_num[:-2]
        
        # Pad to 4 digits if it's numeric
        if s_num.isdigit():
            s_num = s_num.zfill(4)
            
        store_num = s_num
        
        data = {
            'store_number': store_num,
            'company_name': clean_val(row.get('COMPANY')),
            'attn': clean_val(row.get('ATTN')),
            'address1': clean_val(row.get('Address1')),
            'address2': clean_val(row.get('ADD2')),
            'address3': clean_val(row.get('ADD3')),
            'city': clean_val(row.get('City')),
            'state': clean_val(row.get('State')),
            'zip': clean_val(row.get('Zip')),
            'phone': clean_val(row.get('Phone')),
            'email': clean_val(row.get('Email'))
        }
        
        sql = """
            INSERT INTO address_book (
                store_number, company_name, attn, 
                address1, address2, address3, 
                city, state, zip, phone, email, last_updated
            ) VALUES (
                %(store_number)s, %(company_name)s, %(attn)s, 
                %(address1)s, %(address2)s, %(address3)s, 
                %(city)s, %(state)s, %(zip)s, %(phone)s, %(email)s, NOW()
            )
            ON CONFLICT (store_number) DO UPDATE SET
                company_name = EXCLUDED.company_name,
                attn = EXCLUDED.attn,
                address1 = EXCLUDED.address1,
                address2 = EXCLUDED.address2,
                address3 = EXCLUDED.address3,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                zip = EXCLUDED.zip,
                phone = EXCLUDED.phone,
                email = EXCLUDED.email,
                last_updated = NOW();
        """
        cur.execute(sql, data)
        count += 1
        
    conn.commit()
    cur.close()
    conn.close()
    print(f"Import Complete. Processed {count} records.")

if __name__ == "__main__":
    # Hardcoded path for this task
    DEFAULT_PATH = os.path.join(project_root, 'address_book', 'TXRH_Store Directory_DEC 2 2025.xlsx')
    import_excel(DEFAULT_PATH)
