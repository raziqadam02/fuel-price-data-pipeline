from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3
from io import BytesIO
from sqlalchemy import create_engine
import os

# ---------------------------
# Environment / AWS / DB Setup
# ---------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")

S3_RAW_BUCKET = "fuel-price-raw"
S3_PROCESSED_BUCKET = "fuel-price-processed"
RAW_FILE_KEY = "fuelprice.parquet"
PROCESSED_FILE_KEY = "fuelprice_clean.parquet"

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = os.getenv("DB_PORT", "5432")

# ---------------------------
# DAG Default Args
# ---------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'fuel_price_etl',
    default_args=default_args,
    description='ETL DAG for Fuel Price Data',
    schedule_interval='@daily',
    start_date=datetime(2026, 2, 24),
    catchup=False
)

# ---------------------------
# Task 1: Extract from S3
# ---------------------------
def extract_s3_raw():
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    obj = s3.get_object(Bucket=S3_RAW_BUCKET, Key=RAW_FILE_KEY)
    df = pd.read_parquet(BytesIO(obj['Body'].read()))
    df.to_parquet("/tmp/fuelprice_raw.parquet", index=False)
    print(f"Extracted {len(df)} rows from S3 raw bucket")
    return len(df)

extract_task = PythonOperator(
    task_id='extract_s3_raw',
    python_callable=extract_s3_raw,
    dag=dag
)

# ---------------------------
# Task 2: Transform / Clean
# ---------------------------
def transform_clean():
    df = pd.read_parquet("/tmp/fuelprice_raw.parquet")
    # Remove duplicates & invalid prices
    df = df.drop_duplicates()
    df = df[df['price'] > 0]
    df = df.dropna(subset=['station_name','fuel_type','date'])
    df.to_parquet("/tmp/fuelprice_clean.parquet", index=False)
    
    # Upload cleaned file to S3
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    s3.upload_file("/tmp/fuelprice_clean.parquet", S3_PROCESSED_BUCKET, PROCESSED_FILE_KEY)
    print(f"Processed data uploaded to S3 bucket: {S3_PROCESSED_BUCKET}")

transform_task = PythonOperator(
    task_id='transform_clean',
    python_callable=transform_clean,
    dag=dag
)

# ---------------------------
# Task 3: Load into Postgres
# ---------------------------
def load_postgres():
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    df = pd.read_parquet("/tmp/fuelprice_clean.parquet")
    
    # Example: Load fact table only (dim tables can be preloaded)
    df.to_sql("fact_fuel_price", engine, if_exists="append", index=False, method='multi')
    print(f"Loaded {len(df)} rows into Postgres fact_fuel_price table")

load_task = PythonOperator(
    task_id='load_postgres',
    python_callable=load_postgres,
    dag=dag
)

# ---------------------------
# DAG Task Order
# ---------------------------
extract_task >> transform_task >> load_task