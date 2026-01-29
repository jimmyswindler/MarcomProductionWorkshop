import pandas as pd

file_path = 'UPS_Documentation/XML_File_Spec.xlsx'
# Read all sheets and search for "filename" or "naming"
try:
    xl = pd.ExcelFile(file_path)
    for sheet in xl.sheet_names:
        df = pd.read_excel(file_path, sheet_name=sheet)
        mask = df.apply(lambda x: x.astype(str).str.contains('filename|file name|naming', case=False, na=False))
        if mask.any().any():
            print(f"--- Matches in {sheet} ---")
            print(df[mask.any(axis=1)].iloc[:, :5].head())
except Exception as e:
    print(e)
