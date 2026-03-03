import boto3
import pandas as pd
import os
from io import BytesIO
from src.utils.logger import get_logger

logger = get_logger()

# -----------------------------
# Load AWS env variables
# -----------------------------
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# -----------------------------
# Paths
# -----------------------------
RAW_LOCAL_DIR = "data/raw"
PROCESSED_LOCAL_DIR = "data/processed"
RAW_LOCAL_PATH = os.path.join(RAW_LOCAL_DIR, "fuelprice.parquet")
PROCESSED_LOCAL_PATH = os.path.join(PROCESSED_LOCAL_DIR, "fuelprice_clean.parquet")

# Create directories if they don't exist
os.makedirs(RAW_LOCAL_DIR, exist_ok=True)
os.makedirs(PROCESSED_LOCAL_DIR, exist_ok=True)

# -----------------------------
# Download raw parquet from S3 (if needed)
# -----------------------------
bucket_name = "fuel-price-raw-project"
file_key = "fuelprice.parquet"

try:
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_parquet(BytesIO(obj['Body'].read()))
    logger.info(f"Loaded {len(df)} rows from S3 raw parquet")
except Exception as e:
    logger.warning(f"Could not download from S3: {e}")
    # fallback: check if local raw exists
    if os.path.exists(RAW_LOCAL_PATH):
        logger.info(f"Loading local raw parquet: {RAW_LOCAL_PATH}")
        df = pd.read_parquet(RAW_LOCAL_PATH)
    else:
        logger.error("No raw parquet file available!")
        raise

# -----------------------------
# Data Cleaning / Validation
# -----------------------------
df = df.drop_duplicates()
df.columns = df.columns.str.lower().str.replace(" ", "_")
logger.info(f"Data cleaned. {len(df)} rows after deduplication.")

# -----------------------------
# Save cleaned locally
# -----------------------------
df.to_parquet(PROCESSED_LOCAL_PATH, index=False)
logger.info(f"Processed data saved locally at {PROCESSED_LOCAL_PATH}")

# -----------------------------
# Optional: Upload processed to S3
# -----------------------------
processed_bucket = "fuel-price-processed"
try:
    s3.upload_file(PROCESSED_LOCAL_PATH, processed_bucket, "fuelprice_clean.parquet")
    logger.info(f"Processed data uploaded to S3 bucket {processed_bucket}")
except Exception as e:
    logger.warning(f"Could not upload processed file to S3: {e}")