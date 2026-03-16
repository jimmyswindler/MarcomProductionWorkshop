#!/usr/bin/env python3
import sys
import os

project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from shared_lib.database import get_db_connection, init_db

def reset_db():
    print("Warning: Dropping orders, jobs, items, item_boxes, and shipments.")
    conn = get_db_connection()
    if not conn:
        print("DB connection failed.")
        return

    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS item_boxes CASCADE;")
        cur.execute("DROP TABLE IF EXISTS items CASCADE;")
        cur.execute("DROP TABLE IF EXISTS jobs CASCADE;")
        cur.execute("DROP TABLE IF EXISTS shipments CASCADE;")
        cur.execute("DROP TABLE IF EXISTS orders CASCADE;")
        conn.commit()
        print("Tables dropped successfully.")
    except Exception as e:
        print(f"Error dropping tables: {e}")
        conn.rollback()
        return
    finally:
        cur.close()

    print("Re-initializing schema...")
    try:
        init_db(conn)
        print("Database reset complete.")
    except Exception as e:
        print(f"Error initializing schema: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    reset_db()
