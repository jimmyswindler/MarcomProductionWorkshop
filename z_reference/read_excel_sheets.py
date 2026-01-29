import pandas as pd

file_path = 'UPS_Documentation/XML_File_Spec.xlsx'
try:
    xl = pd.ExcelFile(file_path)
    print("Sheet names:", xl.sheet_names)
except Exception as e:
    print(f"Error: {e}")
