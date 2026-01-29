import pandas as pd

file_path = 'UPS_Documentation/XML_File_Spec.xlsx'
sheet_name = 'Shipment Export Schema WS16'

try:
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    # Search entire dataframe for 'ShipID'
    mask = df.apply(lambda x: x.astype(str).str.contains('ShipID', case=False, na=False))
    
    # Get rows where it's found
    results = df[mask.any(axis=1)]
    print("\nFound Rows:")
    print(results)
    
except Exception as e:
    print(f"Error: {e}")
