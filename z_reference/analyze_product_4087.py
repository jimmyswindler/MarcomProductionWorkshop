import psycopg2
import os
import sys
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

DB_NAME = os.getenv("DB_NAME", "marcom_production_suite")
DB_USER = os.getenv("DB_USER", "jimmyswindler")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def get_product_stats(product_id):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()

        query = """
            SELECT
                DATE(o.order_date) as order_day,
                COUNT(DISTINCT o.id) as total_orders,
                SUM(i.quantity_ordered) as total_quantity
            FROM
                items i
            JOIN
                jobs j ON i.job_id = j.id
            JOIN
                orders o ON j.order_id = o.id
            WHERE
                i.product_id = %s
            GROUP BY
                order_day
            ORDER BY
                order_day ASC;
        """
        
        cur.execute(query, (product_id,))
        rows = cur.fetchall()
        
        # Determine columns from cursor
        columns = [desc[0] for desc in cur.description]
        
        cur.close()
        conn.close()
        
        return rows, columns

    except Exception as e:
        print(f"Error: {e}")
        return None, None

def generate_report():
    product_id = '4087'
    rows, columns = get_product_stats(product_id)
    
    if rows:
        df = pd.DataFrame(rows, columns=columns)
        
        # Calculate totals
        total_orders_all_time = df['total_orders'].sum()
        total_quantity_all_time = df['total_quantity'].sum()
        
        print(f"\nAnalysis for Product ID: {product_id}")
        print("=" * 40)
        print(f"Total Days with Orders: {len(df)}")
        print(f"Total Orders (All Time): {total_orders_all_time}")
        print(f"Total Quantity (All Time): {total_quantity_all_time}")
        print("-" * 40)
        print("\nDaily Breakdown:")
        
        # Manual table printing
        header = f"{'Order Day':<15} | {'Orders':<10} | {'Quantity':<10}"
        print(header)
        print("-" * len(header))
        for _, row in df.iterrows():
            print(f"{str(row['order_day']):<15} | {row['total_orders']:<10} | {row['total_quantity']:<10}")
            
        # Also save to a markdown file for the user
        report_path = os.path.join(os.path.dirname(__file__), 'product_4087_report.md')
        with open(report_path, 'w') as f:
            f.write(f"# Product {product_id} Analysis\n\n")
            f.write(f"**Total Orders:** {total_orders_all_time}\n\n")
            f.write(f"**Total Quantity:** {total_quantity_all_time}\n\n")
            f.write("## Daily Breakdown\n\n")
            
            # Manual Markdown table
            f.write("| Order Day | Orders | Quantity |\n")
            f.write("|---|---|---|\n")
            for _, row in df.iterrows():
                f.write(f"| {row['order_day']} | {row['total_orders']} | {row['total_quantity']} |\n")
            
        print(f"\nReport saved to: {report_path}")

    else:
        print(f"No data found for product {product_id}")

if __name__ == "__main__":
    generate_report()
