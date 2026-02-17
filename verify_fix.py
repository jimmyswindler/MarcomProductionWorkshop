
import sys
import os
# Add project root to sys.path
sys.path.append(os.getcwd())

from admin_web_app.app import app, api_chart_data

def verify_fix():
    print("--- Verifying api_chart_data Logic ---")
    
    with app.test_request_context('/api/chart-data?days=30'):
        try:
            # api_chart_data returns a dict directly in the code I viewed
            # (Flask usually converts dict to json response if returned from view, 
            # but when called directly it returns the return value)
            response = api_chart_data()
            print("Successfully called api_chart_data")
            
            if isinstance(response, dict):
                 totals = response.get('totals')
                 print(f"Totals returned: {totals}")
                 if totals and 'overdue' in totals:
                     print(f"Overdue Total: {totals['overdue']}")
                     print("SUCCESS: 'totals' is defined and contains 'overdue'")
                 else:
                     print("FAILURE: 'totals' missing or empty")
            else:
                 print(f"Unexpected response type: {type(response)}")

        except Exception as e:
            print(f"FAILURE: Exception raised: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    verify_fix()
