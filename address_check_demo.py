
try:
    import zipcodes
except ImportError:
    print("Error: 'zipcodes' library is not installed.")
    print("Try running: pip install zipcodes")
    exit(1)

def check_address(city, state, zipcode):
    print(f"Checking: {city}, {state} {zipcode}")
    
    # 1. basic validation of zip
    is_valid = zipcodes.is_real(zipcode)
    print(f"Is Zip Real? {is_valid}")
    
    if is_valid:
        # 2. Match city/state
        details = zipcodes.matching(zipcode)
        if details:
            for d in details:
                print(f"  - Found: {d['city']}, {d['state']}")
            
            # Check if city matches (loose check)
            matched = False
            for d in details:
                if d['city'].lower() == city.lower() or city.lower() in d['city'].lower():
                    matched = True
            print(f"City Match Found? {matched}")
        else:
            print("No details found for this zip.")
    else:
        print("Invalid Zip Code format or not in database.")

if __name__ == "__main__":
    print("--- Address Validation Demo ---")
    check_address("Louisville", "KY", "40203")
    print("-" * 20)
    check_address("New York", "NY", "90210") # Mismatch intentional
    print("-" * 20)
    check_address("Beverly Hills", "CA", "90210")
