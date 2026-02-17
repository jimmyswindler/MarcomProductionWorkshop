
import sys
import os
sys.path.append(os.getcwd())
from shipping_web_app.app.services import shipment_service

def verify():
    print("Fetching recent shipments...")
    data, error = shipment_service.get_recent_shipments(limit=5)
    
    if error:
        print(f"Error: {error}")
        return

    print(f"Retrieved {len(data)} shipments.")
    for s in data:
        print(f"UID: {s['shipment_uid']}")
        print(f"  Tracking: {s['tracking_number']}")
        print(f"  Contents: {s['contents']}")
        print(f"  Marcom Status: {s['marcom_sync_status']}")
        print("-" * 20)

if __name__ == "__main__":
    try:
        verify()
    except Exception as e:
        print(f"Verification Failed: {e}")
