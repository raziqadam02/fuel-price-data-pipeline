import pandas as pd
import os

# Paths
input_file = "data/processed/fuelprice_clean.parquet"
output_csv = "data/processed/fuelprice_clean.csv"

# Check if the file exists
if not os.path.exists(input_file):
    raise FileNotFoundError(f"{input_file} not found!")

# Read the Parquet file
df = pd.read_parquet(input_file)

# Show summary stats
print("=== Data Overview ===")
print(df.head(10))  # first 10 rows
print("\n=== Data Info ===")
print(df.info())    # column types, non-null counts
print("\n=== Descriptive Stats ===")
print(df.describe(include='all'))  # basic statistics

# Export to CSV
df.to_csv(output_csv, index=False)
print(f"\n Cleaned data exported to {output_csv}")