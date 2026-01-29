import pandas as pd

file_path = 'UPS_Documentation/XML_File_Spec.xlsx'
sheet_name = 'Shipment Export Schema WS16'

try:
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    print("Head:")
    print(df.head(10))
except Exception as e:
    print(f"Error: {e}")
