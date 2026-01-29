import pandas as pd

file_path = 'UPS_Documentation/XML_File_Spec.xlsx'
sheet_name = 'Auto XML Process Schema WS 9.0+'

try:
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    # Print cols
    print("Columns:", df.columns.tolist())
    # Search for Weight in the first few columns
    print("\n--- Weight ---")
    print(df[df.astype(str).apply(lambda x: x.str.contains('Weight', case=False, na=False)).any(axis=1)].head(5))
    
    print("\n--- CompanyOrName ---")
    print(df[df.astype(str).apply(lambda x: x.str.contains('CompanyOrName', case=False, na=False)).any(axis=1)].head(5))

except Exception as e:
    print(f"Error: {e}")
