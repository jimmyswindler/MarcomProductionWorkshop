import sys
import os

sys.path.append(os.path.join(os.getcwd(), 'shipping_web_app'))

from app.services import shipment_service

def verify():
    print("Testing get_shipping_cartons()...")
    cartons, err = shipment_service.get_shipping_cartons()
    if err:
        print(f"Error: {err}")
    else:
        print(f"Success! Found {len(cartons)} cartons.")
        for k, v in list(cartons.items())[:3]:
            print(f"  {k}: {v} lbs")

    print("\nTesting WorldShip XML logic locally...")
    # Mock data that app.js would send
    orders = [{
        "order_number": "TEST-123",
        "ship_to": {
            "name": "Test Name",
            "company": "Test Company",
            "address1": "123 Test St",
            "city": "Testville",
            "state": "TS",
            "zip": "12345",
            "country": "US",
            "account_number": "Y76383"
        }
    }]
    # We won't actually hit the DB to mark as packed to avoid dirtying it,
    # so we'll just test the generate_worldship_xml directly 
    # since process_shipment_logic does DB writes.
    
    # We pretend package_list from app.js calculated 5.5 lbs total
    packages = [
        {"weight": 5.5, "L": 10, "W": 10, "H": 10}
    ]
    xml = shipment_service.generate_worldship_xml({"orders": orders}, packages, "1234")
    
    if "<Weight>6</Weight>" in xml:
         print("Success! XML contains <Weight>6</Weight> (ceil of 5.5).")
         # print(xml)
    else:
         print("Failed to find <Weight>6</Weight> in XML.")
         print(xml)

if __name__ == '__main__':
    verify()
