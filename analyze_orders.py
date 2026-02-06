
import sys
import os
import pandas as pd
from shared_lib.database import get_db_connection

def analyze_orders():
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to database")
        return

    try:
        # Check if 'order_date' column exists and its type or just query it
        # Assuming table is 'orders' and column is 'order_date'
        query = """
            SELECT order_date::date as day, count(*) as order_count 
            FROM orders 
            WHERE order_date IS NOT NULL
            GROUP BY day 
            HAVING count(*) > 0
            ORDER BY day
        """
        
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            print("No order data found.")
            return

        # Basic Stats
        average_daily = df['order_count'].mean()
        
        # Top 5
        top_5 = df.nlargest(5, 'order_count')
        
        # Bottom 5
        bottom_5 = df.nsmallest(5, 'order_count')
        
        print(f"Average Orders Per Day: {average_daily:.2f}")
        print("\nTop 5 Days:")
        print(top_5.to_string(index=False))
        print("\nBottom 5 Days:")
        print(bottom_5.to_string(index=False))
        
        # Generate data for chart
        # We will output a JSON structure or similar to be used for charting if needed
        # For now, let's create a simple HTML file with Chart.js
        
        dates = df['day'].astype(str).tolist()
        counts = df['order_count'].tolist()
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Order Volume Report</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{ font-family: sans-serif; padding: 20px; }}
        .container {{ width: 800px; margin: 0 auto; }}
        h2 {{ text-align: center; }}
        .stats {{ display: flex; justify-content: space-around; margin-bottom: 20px; }}
        .stat-box {{ border: 1px solid #ddd; padding: 15px; border-radius: 8px; text-align: center; }}
    </style>
</head>
<body>
    <div class="container">
        <h2>Daily Order Volume</h2>
        
        <div class="stats">
            <div class="stat-box">
                <strong>Average Orders/Day</strong><br>
                {average_daily:.2f}
            </div>
        </div>

        <canvas id="orderChart"></canvas>
    </div>

    <script>
        const ctx = document.getElementById('orderChart').getContext('2d');
        const orderChart = new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: {dates},
                datasets: [{{
                    label: 'Orders',
                    data: {counts},
                    borderColor: 'rgb(75, 192, 192)',
                    tension: 0.1,
                    fill: false
                }}]
            }},
            options: {{
                scales: {{
                    y: {{
                        beginAtZero: true
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>
        """
        
        with open("order_report.html", "w") as f:
            f.write(html_content)
        
        print("\nReport generated: order_report.html")

    except Exception as e:
        print(f"Error analyzing orders: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    analyze_orders()
