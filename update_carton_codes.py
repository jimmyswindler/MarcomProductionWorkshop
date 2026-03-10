import os
import sys

# Ensure this script runs from the current directory context and can import shared_lib
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared_lib.database import get_db_connection

def update_cartons():
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to database.")
        sys.exit(1)
        
    cur = conn.cursor()
    
    code_updates = {
        "#105": "C05",
        "#115": "C15",
        "#116": "C16",
        "#118": "C18",
        "#123": "C23",
        "#145": "C45",
        "#160": "C60",
        "#999": "C99"
    }
    
    try:
        cur.execute("BEGIN;")
        for old_code, new_code in code_updates.items():
            print(f"Updating {old_code} to {new_code}...")
            # Using DO UPDATE to avoid conflicts if script is re-run or partially run
            cur.execute("""
                UPDATE shipping_cartons 
                SET code = %s 
                WHERE code = %s;
            """, (new_code, old_code))
        
        cur.execute("COMMIT;")
        print("Carton codes updated successfully.")
        
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Error updating carton codes: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    update_cartons()
