import pandas as pd
import os

file_path = 'UPS_Documentation/XML_File_Spec.xlsx'

try:
    # Load the excel file, just the first few rows to see structure
    df = pd.read_excel(file_path, sheet_name=0) 
    print("Columns:", df.columns)
    print(df.head(10))
    
    # Search for 'Weight' or 'CompanyOrName' constraints
    print("\nSearch for 'Weight':")
    print(df[df.astype(str).apply(lambda x: x.str.contains('Weight', case=False, na=False)).any(axis=1)])
except Exception as e:
    print(f"Error: {e}")
