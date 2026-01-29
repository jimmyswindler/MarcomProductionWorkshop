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
