
import requests
import time
import sys

def verify():
    base_url = "http://localhost:5002"
    
    # Wait for server
    time.sleep(2)
    
    # 1. Dashboard
    try:
        print("Checking Dashboard...")
        r = requests.get(base_url + "/")
        if r.status_code != 200:
            print(f"FAILED: Dashboard returned {r.status_code}")
            return False
            
        content = r.text
        
        checks = [
            ("Production Workshop Administration", "Header Text"),
            ('id="productionChart"', "Timeline Chart Canvas"),
            ("Address Validation Report", "Validation Widget Header"),
            ("Status Lookup", "Status Lookup Placeholder"),
            ("Recent Auto-Corrections", "Corrections Widget Header")
        ]
        
        for text, desc in checks:
            if text in content:
                print(f"  [PASS] {desc}")
            else:
                print(f"  [FAIL] {desc} not found in response.")
                return False
                
    except Exception as e:
        print(f"FAILED: Connection error: {e}")
        return False

    # 2. Exceptions Page
    try:
        print("\nChecking Exceptions Page...")
        r = requests.get(base_url + "/exceptions")
        if r.status_code != 200:
            print(f"FAILED: Exceptions returned {r.status_code}")
            return False
            
        if "Address Validation Exceptions" in r.text:
            print("  [PASS] Headline Text")
        else:
            print("  [FAIL] Headline Text not found.")
            return False
            
    except Exception as e:
        print(f"FAILED: Connection error: {e}")
        return False

    return True

if __name__ == "__main__":
    if verify():
        print("\nVerification SUCCESS")
    else:
        print("\nVerification FAILED")
        sys.exit(1)
