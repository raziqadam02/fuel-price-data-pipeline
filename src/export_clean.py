import pandas as pd
import os

input_file = "data/processed/fuelprice_clean.parquet"
output_csv = "data/processed/fuelprice_clean.csv"

if not os.path.exists(input_file):
    raise FileNotFoundError(f"{input_file} not found!")

df = pd.read_parquet(input_file)

print("=== Data Overview ===")
print(df.head(10))  
print("\n=== Data Info ===")
print(df.info())    
print("\n=== Descriptive Stats ===")
print(df.describe(include='all'))  

df.to_csv(output_csv, index=False)
print(f"\n Cleaned data exported to {output_csv}")