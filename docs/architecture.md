# Architecture Diagram

## ETL Pipeline Flow (Dockerized & Cloud Based)

```
                +---------------------------+
                |   Amazon S3 (Raw Bucket)  |
                |   Raw Parquet Dataset     |
                +---------------------------+
                              |
                              v
                +---------------------------+
                |      Docker Container     |
                |   (Python ETL Pipeline)   |
                |---------------------------|
                | - Extract (boto3)         |
                | - Clean (pandas)          |
                | - Deduplicate             |
                | - Validate row counts     |
                +---------------------------+
                              |
                              v
                +---------------------------+
                |   Clean Parquet Dataset   |
                |   (Local Container FS)    |
                +---------------------------+
                              |
                              v
                +---------------------------+
                | Amazon S3 (Processed)     |
                | Cleaned Parquet Output    |
                +---------------------------+
```

## Overview

This project follows a modern cloud-based data engineering pattern:

1. Data Source

- Raw fuel price dataset stored in Amazon S3
- File format: Parquet (columnar, analytics-optimized)

2. Processing Layer

- Entire ETL pipeline runs inside a Docker container
- Built using:

Dockerfile
docker-compose.yml

- AWS credentials injected securely via .env
- No hardcoded secrets in code

3. Transformation

Entire ETL pipeline runs inside a Docker container

- Built using:
- Dockerfile
- docker-compose.yml
- AWS credentials injected securely via .env
- No hardcoded secrets in code

4. Data Output

- Cleaned Parquet saved locally inside container
- Uploaded to separate S3 processed bucket
- Optional CSV export for reporting/demo