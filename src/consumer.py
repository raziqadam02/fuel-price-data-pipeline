import os
import pandas as pd
from sqlalchemy import create_engine

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

engine = create_engine(DATABASE_URL)

df = pd.read_parquet("data/processed/fuelprice_clean.parquet")

df.to_sql(
    "fact_fuel_price",
    con=engine,
    if_exists="append",  
    index=False,
    method="multi"
)

print("Data loaded into PostgreSQL successfully")