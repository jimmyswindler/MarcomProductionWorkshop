from shipping_web_app.app import generate_worldship_xml
import re

# Mock Data
mock_shipment_data = {
    "orders": [
        {
            "order_number": "TXRH-1415931",
            "ship_to": {
                "name": "Original Name",
                "company": "Original Company",
                "address1": "123 Main St",
                "city": "Anytown",
                "state": "KY",
                "zip": "40202",
                "country": "US",
                "account_number": "Y76383"
            }
        }
    ]
}

mock_packages = [
    {"weight": 5.2, "L": 14.0, "W": 14.0, "H": 10.0}
]

print("--- Test Case 1: Store #244 (Logic applied) ---")
xml_out_244 = generate_worldship_xml(mock_shipment_data, mock_packages, store_number_arg="244")

# Checks
if "Texas Roadhouse #244" in xml_out_244:
    print("PASS: Company Name override success")
else:
    print("FAIL: Company Name override failed")

if "Store Manager" in xml_out_244:
    print("PASS: Attention override success")
else:
    print("FAIL: Attention override failed")

if "<Weight>6</Weight>" in xml_out_244:
    print("PASS: Weight rounding success (5.2 -> 6)")
else:
    print("FAIL: Weight rounding failed")

if "<ThirdParty>" in xml_out_244 and "<BillThirdParty>" not in xml_out_244:
    print("PASS: ThirdParty structure correct")
else:
    print("FAIL: ThirdParty structure incorrect")
    
if 'ProcessStatus="Y"' in xml_out_244:
    print("PASS: ProcessStatus correct")
else:
    print("FAIL: ProcessStatus incorrect")

print("\n--- Test Case 2: Store #1500 (No Logic) ---")
xml_out_1500 = generate_worldship_xml(mock_shipment_data, mock_packages, store_number_arg="1500")

if "Original Company" in xml_out_1500:
    print("PASS: Standard Company preserved")
else:
    print("FAIL: Standard Company overwritten")

if "Original Name" in xml_out_1500:
    print("PASS: Standard Name preserved")
else:
    print("FAIL: Standard Name overwritten")
