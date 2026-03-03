# Fuel Price ETL Pipeline (Dockerized & Cloud-Integrated)

##  Project Objective

This project builds a fully containerized end-to-end ETL pipeline that processes raw fuel price data stored in Parquet format, applies data cleaning transformations, and uploads the processed dataset to Amazon S3.

The entire pipeline runs inside Docker, ensuring portability, reproducibility, and environment consistency across machines.

This project demonstrates practical data engineering skills including:

- Working with Parquet files
- Data transformation using Python (pandas)
- AWS S3 integration (raw → processed)
- Docker containerization
- Cloud-ready architecture design
- Data validation and quality checks
- Format conversion (Parquet → CSV ready)

---

##  Architecture

S3 (Raw Parquet)
→ Dockerized Python ETL
→ Data Cleaning (pandas)
→ Processed Parquet
→ S3 (Processed Bucket)

---

## Containerized Architecture

The pipeline runs inside a Docker container built using:

- Dockerfile
- docker-compose.yml
- .env for AWS credentials

To run the full pipeline:

<docker compose up --build>

The container:

- Connects to AWS S3
- Downloads raw Parquet file
- Cleans and deduplicates data
- Saves processed Parquet locally
- Uploads processed file back to S3
- Exits successfully (exit code 0)

---

## Cloud Integration

- Connects to AWS S3
- Downloads raw Parquet file
- Cleans and deduplicates data
- Saves processed Parquet locally
- Uploads processed file back to S3
- Exits successfully (exit code 0)

---
##  Tech Stack

- Python (pandas, pyarrow, boto3)
- Docker & Docker Compose
- Parquet
- AWS S3
- Git & GitHub

---

##  Pipeline Steps

1. Extract raw fuel price dataset from S3 (Parquet format)
2. Clean and standardize columns using pandas
3. Remove duplicates and validate row counts
4. Save cleaned dataset as Parquet
5. Upload processed dataset to S3
6. (Optional) Export to CSV for reporting

---

##  Output Files

- Cleaned Parquet dataset (local + S3)
- Optional CSV export
- Dockerized ETL logs
- Architecture documentation

---

## Sample Log Output

Loaded 895 rows from S3 raw parquet
Data cleaned. 895 rows after deduplication.
Processed data saved locally at data/processed/fuelprice_clean.parquet
Processed data uploaded to S3 bucket fuel-price-processed
fuel_etl exited with code 0

---


##  Future Improvements

- Add Airflow orchestration
- Implement incremental data loading
- Add unit tests
- Deploy to AWS ECS
- Add CI/CD pipeline (GitHub Actions)
- Build dashboard (Power BI / Tableau)

---

##  Author

Raziq Adam