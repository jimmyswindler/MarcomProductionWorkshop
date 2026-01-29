import pandas as pd

file_path = 'UPS_Documentation/XML_File_Spec.xlsx'
sheet_name = 'Shipment Export Schema WS16'

try:
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    # Search for ShipID
    print("\n--- ShipID Search ---")
    # Print adjacent columns if found
    matches = df[df.astype(str).apply(lambda x: x.str.contains('ShipID', case=False, na=False)).any(axis=1)]
    print(matches.dropna(axis=1, how='all').head(10))
    
    # Check for specific description columns usually found in these specs (like "Element Name", "Description")
    # I'll just dump a few rows around the match if structure is unclear
    
except Exception as e:
    print(f"Error: {e}")
