import requests

BASE_URL = "http://127.0.0.1:5001/api"

def test_missing_weight():
    payload = {
        "orders": [{"order_number": "TEST-123", "ship_to": {"name": "Test User"}}],
        "scanned_barcodes": ["99999999Z"], 
        "package_list": [
            {"id": "#115"} 
        ]
    }
    
    print("Testing backend rejection of unknown weights...")
    try:
        response = requests.post(f"{BASE_URL}/shipment/process", json=payload)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_missing_weight()
