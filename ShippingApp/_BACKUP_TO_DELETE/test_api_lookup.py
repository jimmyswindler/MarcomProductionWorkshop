
import sys
import json
import os

# Add ShippingApp to path so we can import app
sys.path.append('/Users/jimmyswindler/Desktop/ShippingApp')

from app import app

def run_tests():
    client = app.test_client()
    
    print("--- Test 1: Lookup by Job Ticket (CL185291) ---")
    res = client.get('/api/order/CL185291')
    if res.status_code == 200:
        data = res.get_json()
        print(f"SUCCESS: Found Job Ticket. Order #: {data.get('order_number')}")
        # Verify specific fields for single job
        if data.get('order_number') == 'CL185291':
            print("  - Verified Order Number matches Job Ticket (Standard behavior)")
        else:
            print(f"  - WARNING: Expected CL185291, got {data.get('order_number')}")
    else:
        print(f"FAILURE: Status {res.status_code} - {res.data}")

    print("\n--- Test 2: Lookup by Order Number (TXRH-1415887) ---")
    res = client.get('/api/order/TXRH-1415887')
    if res.status_code == 200:
        data = res.get_json()
        print(f"SUCCESS: Found Order. Order #: {data.get('order_number')}")
        
        # Verify it is the Order Number
        if data.get('order_number') == 'TXRH-1415887':
             print("  - Verified Order Number matches input.")
        else:
             print(f"  - FAILURE: Expected TXRH-1415887, got {data.get('order_number')}")
             
        # Verify aggregation
        barcodes = data.get('expected_barcodes', [])
        print(f"  - Found {len(barcodes)} barcodes.")
        
        items = data.get('line_items', [])
        print(f"  - Found {len(items)} line items.")
        
        if len(items) > 1:
             print("  - Confirmed multiple items retrieved (Aggregation likely working).")
        else:
             print("  - Note: Only 1 item found (Could be correct if total order only has 1 item).")
             
        # Verify Job Ticket Field
        if 'job_ticket' in items[0]:
             print(f"  - SUCCESS: Field 'job_ticket' present. Sample value: {items[0]['job_ticket']}")
        else:
             print("  - FAILURE: Field 'job_ticket' MISSING in line items.")
             
    else:
        print(f"FAILURE: Status {res.status_code} - {res.data}")

    print("\n--- Test 3: Invalid ID ---")
    res = client.get('/api/order/INVALID-999')
    if res.status_code == 404:
        print("SUCCESS: correctly returned 404 for invalid ID")
    else:
        print(f"FAILURE: Expected 404, got {res.status_code}")

if __name__ == "__main__":
    run_tests()
