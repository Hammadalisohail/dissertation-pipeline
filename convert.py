import pandas as pd

print("Reading Excel file... this may take 1-2 minutes")

# Read both sheets
df1 = pd.read_excel(
    r"D:\dissertation-pipeline\data\online+retail+ii\online_retail_II.xlsx",
    sheet_name="Year 2009-2010"
)

df2 = pd.read_excel(
    r"D:\dissertation-pipeline\data\online+retail+ii\online_retail_II.xlsx",
    sheet_name="Year 2010-2011"
)

# Combine both sheets
df = pd.concat([df1, df2], ignore_index=True)

print(f"Total records: {len(df)}")
print(f"Columns: {list(df.columns)}")

# Save as CSV
output_path = r"D:\dissertation-pipeline\data\online_retail.csv"
df.to_csv(output_path, index=False)

print(f"CSV saved successfully!")
print(f"File location: {output_path}")