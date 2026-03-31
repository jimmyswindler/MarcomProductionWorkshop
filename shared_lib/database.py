import psycopg2
import psycopg2.extras
from .config import get_env_var

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=get_env_var("DB_NAME", "marcom_production_suite"),
            user=get_env_var("DB_USER", "jimmyswindler"),
            host=get_env_var("DB_HOST", "localhost"),
            port=get_env_var("DB_PORT", "5432")
        )
        return conn
    except Exception as e:
        print(f"DB Connection Error: {e}")
        return None

def get_real_dict_cursor(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

def init_db(conn):
    """
    Initializes the database schema if it doesn't exist.
    Also handles schema migrations (adding new columns).
    """
    cur = conn.cursor()
    try:
        # --- 1. TABLES ---
        
        # Orders Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                order_number TEXT UNIQUE NOT NULL,
                order_date TIMESTAMP,
                ship_date TIMESTAMP,
                ship_to_company TEXT,
                ship_to_name TEXT,
                address1 TEXT,
                address2 TEXT,
                address3 TEXT,
                address4 TEXT,
                city TEXT,
                state TEXT,
                zip TEXT,
                country TEXT,
                address_validated BOOLEAN DEFAULT FALSE,
                address_validation_status TEXT,
                address_validation_details JSONB,
                original_address JSONB,
                is_residential BOOLEAN DEFAULT FALSE,
                store_number TEXT,
                production_status TEXT DEFAULT 'NEW',
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

        # Jobs Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id SERIAL PRIMARY KEY,
                job_ticket_number TEXT UNIQUE NOT NULL,
                order_id INTEGER REFERENCES orders(id) ON DELETE CASCADE,
                project_description TEXT,
                general_description TEXT,
                paper_description TEXT,
                press_instructions TEXT,
                bindery_instructions TEXT,
                shipping_instructions TEXT,
                special_instructions TEXT,
                production_status TEXT DEFAULT 'NEW',  -- NEW, READY, IN_PROCESS, IN_PRODUCTION, SHIPPED
                production_batch_id TEXT,
                print_filename TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

        # Items Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id SERIAL PRIMARY KEY,
                order_item_id TEXT UNIQUE NOT NULL,
                job_id INTEGER REFERENCES jobs(id) ON DELETE CASCADE,
                product_id TEXT,
                product_name TEXT,
                product_description TEXT,
                sku TEXT,
                sku_description TEXT,
                quantity_ordered INTEGER,
                cost_center TEXT,
                file_url TEXT,
                print_filename TEXT, -- To track generated file per item if needed
                job_ticket_display_id TEXT, 
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

        # Item Boxes Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS item_boxes (
                id SERIAL PRIMARY KEY,
                order_item_id TEXT REFERENCES items(order_item_id) ON DELETE CASCADE,
                box_sequence INT,
                barcode_value TEXT,
                shipment_uid TEXT,
                status TEXT,
                packed_at TIMESTAMP,
                UNIQUE(order_item_id, box_sequence)
            );
        """)

        # Shipments Table (if not already existing)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shipments (
                id SERIAL PRIMARY KEY,
                shipment_uid TEXT UNIQUE NOT NULL,
                order_number TEXT,
                order_id INTEGER REFERENCES orders(id),
                tracking_number TEXT,
                carrier TEXT,
                status TEXT,
                marcom_sync_status TEXT,
                marcom_response_message TEXT,
                packing_slip_id TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        
        # Address Book Table (if not already existing)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS address_book (
                id SERIAL PRIMARY KEY,
                store_number TEXT UNIQUE,
                company_name TEXT,
                attn TEXT,
                address1 TEXT,
                address2 TEXT,
                address3 TEXT,
                city TEXT,
                state TEXT,
                zip TEXT,
                last_updated TIMESTAMP DEFAULT NOW()
            );
        """)

        # Shipping Stations (New)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shipping_stations (
                id SERIAL PRIMARY KEY,
                station_id TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL,
                smb_path TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)


        # --- 2. MIGRATIONS (Add Columns if Missing) ---
        
        # Add production_status to ORDERS if missing
        cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS production_status TEXT DEFAULT 'NEW';")
        cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS cost_center TEXT;")
        
        # Add production_status, batch_id to JOBS if missing
        cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS production_status TEXT DEFAULT 'NEW';")
        cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS production_batch_id TEXT;")
        cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS print_filename TEXT;")

        # Add generic print_filename to ITEMS if missing
        cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS print_filename TEXT;")

        # Add job_ticket_display_id to ITEMS if missing
        cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS job_ticket_display_id TEXT;")

        # Add shipment_uid to ITEM_BOXES if missing
        cur.execute("ALTER TABLE item_boxes ADD COLUMN IF NOT EXISTS shipment_uid TEXT;")
        cur.execute("ALTER TABLE item_boxes ADD COLUMN IF NOT EXISTS status TEXT;")
        cur.execute("ALTER TABLE item_boxes ADD COLUMN IF NOT EXISTS packed_at TIMESTAMP;")

        # Add actual_ship_date to ORDERS if missing
        cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS actual_ship_date TIMESTAMP;")

        # Add ship_date to SHIPMENTS if missing
        cur.execute("ALTER TABLE shipments ADD COLUMN IF NOT EXISTS ship_date TIMESTAMP;")

        # Add carrier and status to SHIPMENTS if missing (adapting to actual DB schema)
        cur.execute("ALTER TABLE shipments ADD COLUMN IF NOT EXISTS carrier TEXT;")
        cur.execute("ALTER TABLE shipments ADD COLUMN IF NOT EXISTS status TEXT;")

        # Add new shipments synchronization fields
        cur.execute("ALTER TABLE shipments ADD COLUMN IF NOT EXISTS order_number TEXT;")
        cur.execute("ALTER TABLE shipments ADD COLUMN IF NOT EXISTS marcom_sync_status TEXT;")
        cur.execute("ALTER TABLE shipments ADD COLUMN IF NOT EXISTS marcom_response_message TEXT;")
        cur.execute("ALTER TABLE shipments ADD COLUMN IF NOT EXISTS packing_slip_id TEXT;")
        cur.execute("ALTER TABLE shipments ADD COLUMN IF NOT EXISTS station_id TEXT;")

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Schema Initialization Error: {e}")
        raise
    finally:
        cur.close()
