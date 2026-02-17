
import sys
import os
# Add parent directory to sys.path to find shared_lib
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.shipment_service import get_recent_shipments
from shared_lib.database import get_db_connection

def verify():
    print("Testing get_recent_shipments...")
    feed, error = get_recent_shipments(limit=5)
    if error:
        print(f"Error: {error}")
        return

    print(f"Fetched {len(feed)} items.")
    for item in feed:
        print(f"UID: {item.get('shipment_uid')}")
        print(f"Carrier: {item.get('carrier')}")
        print(f"Tracking: {item.get('tracking_number')}")
        print(f"Contents: {item.get('contents')}")
        print("-" * 20)

if __name__ == "__main__":
    verify()
