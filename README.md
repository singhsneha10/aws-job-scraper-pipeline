# AWS Job Scraper Pipeline — Glue + S3 + Redshift

A production-style **cloud ETL pipeline** built on **AWS Glue, S3, and Redshift** that scrapes live job listings from the Himalayas API daily, lands raw data in S3, and loads cleaned records into a Redshift data warehouse — fully automated and deduplicated across runs.

This is the AWS-native rebuild of my original [local Python job scraper](https://github.com/singhsneha10/data-engineering-portfolio/tree/main/webscraping) which used PostgreSQL and APScheduler.

---

## Architecture

```
Himalayas API
      │
      ▼
┌─────────────────────────────────────┐
│  AWS Glue Job 1: extract_to_s3.py  │
│  Fetches + cleans job listings      │
│  Writes newline-delimited JSON      │
└─────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────┐
│  AWS S3 — Bronze Layer              │
│  bronze/job_listings/YYYY/MM/DD/    │
│  Partitioned by run date            │
└─────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────┐
│  AWS Glue Job 2: load_to_redshift  │
│  Reads today's S3 files             │
│  Inserts with deduplication         │
└─────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────┐
│  AWS Redshift                       │
│  jobs.job_listings table            │
│  Analytics-ready, deduplicated      │
└─────────────────────────────────────┘
```

---

## Tech Stack

| Service | Purpose |
|---------|---------|
| AWS Glue | Serverless ETL — runs both pipeline scripts on schedule |
| AWS S3 | Bronze layer — stores raw JSON partitioned by date |
| AWS Redshift | Data warehouse — final analytics-ready destination |
| Himalayas API | Source — live remote job listings |
| Python | Glue job runtime — requests, boto3, psycopg2 |

---

## Project Structure

```
aws-job-scraper-pipeline/
├── glue_jobs/
│   ├── extract_to_s3.py        # Glue Job 1: API → S3 Bronze
│   └── load_to_redshift.py     # Glue Job 2: S3 → Redshift
├── scripts/
│   └── redshift_ddl.sql        # Creates jobs schema and job_listings table
├── requirements.txt
└── README.md
```

---

## S3 Structure

```
s3://your-bucket/
└── bronze/
    └── job_listings/
        └── 2026/
            └── 05/
                └── 07/
                    └── jobs_120000.json
```

Each daily run creates its own date-partitioned folder. This makes it easy to reprocess a specific day or query historical raw data directly via Athena if needed.

---

## Pipeline Flow

### Glue Job 1 — extract_to_s3.py
1. Calls Himalayas API (`q=data+engineer&limit=100`)
2. Cleans each record — standardises salary, location, job type
3. Skips incomplete records (missing title, company, or URL)
4. Writes valid records as newline-delimited JSON to S3 Bronze layer
5. Partitions output by run date (`YYYY/MM/DD`)

### Glue Job 2 — load_to_redshift.py
1. Lists today's JSON files from S3 Bronze layer
2. Reads and parses every record
3. Inserts into Redshift using `WHERE NOT EXISTS` — idempotent across reruns
4. Logs inserted vs skipped (duplicate) counts per run

---

## Setup Instructions

### Prerequisites
- AWS account with Glue, S3, and Redshift access
- Redshift cluster provisioned and accessible
- IAM role with S3 read/write + Glue execution permissions

### Step 1 — Create Redshift table
Run `scripts/redshift_ddl.sql` in Redshift Query Editor v2.

### Step 2 — Upload Glue scripts to S3
```
s3://your-bucket/glue-scripts/extract_to_s3.py
s3://your-bucket/glue-scripts/load_to_redshift.py
```

### Step 3 — Create Glue Job 1 (Extract)
- Runtime: Python Shell
- Script: `s3://your-bucket/glue-scripts/extract_to_s3.py`
- Job parameters:
  - `--S3_BUCKET` → your bucket name

### Step 4 — Create Glue Job 2 (Load)
- Runtime: Python Shell
- Script: `s3://your-bucket/glue-scripts/load_to_redshift.py`
- Job parameters:
  - `--S3_BUCKET` → your bucket name
  - `--REDSHIFT_HOST` → your Redshift cluster endpoint
  - `--REDSHIFT_DB` → database name
  - `--REDSHIFT_USER` → username
  - `--REDSHIFT_PASSWORD` → password
  - `--REDSHIFT_PORT` → 5439

### Step 5 — Schedule with Glue Triggers
- Trigger Job 1 daily at 08:00 UTC
- Trigger Job 2 on Job 1 completion (event-based trigger) — ensures Job 2 only runs after Job 1 succeeds

---

## Key Engineering Decisions

**Why S3 as intermediate layer?**
Landing raw data in S3 first decouples extraction from loading. If Redshift is unavailable, raw data is safe in S3 and can be reloaded without calling the API again. This is proper ELT pattern.

**Why newline-delimited JSON?**
One record per line makes files directly queryable by Athena and readable by Glue without any additional parsing configuration. Also allows partial file processing if a load fails midway.

**Why date partitioning in S3?**
Partitioning by `YYYY/MM/DD` means Athena and Glue only scan relevant date folders instead of the entire dataset — reduces both query cost and processing time as data grows.

**Why WHERE NOT EXISTS instead of ON CONFLICT?**
Redshift does not support PostgreSQL's `ON CONFLICT DO NOTHING` syntax. `WHERE NOT EXISTS` achieves identical idempotent insert behaviour and is the standard Redshift deduplication pattern.

**Why Glue event-based trigger for Job 2?**
Scheduling Job 2 on a fixed time risks it running before Job 1 finishes. An event-based trigger fires Job 2 only on successful completion of Job 1 — guarantees correct pipeline order.

---

## Comparison: Local vs AWS Version

| Feature | Local Version | AWS Version |
|---------|--------------|-------------|
| Compute | Local machine | AWS Glue (serverless) |
| Storage | PostgreSQL only | S3 (Bronze) + Redshift |
| Scheduling | APScheduler | Glue Triggers |
| Deduplication | ON CONFLICT DO NOTHING | WHERE NOT EXISTS |
| Raw data preserved | No | Yes (S3 Bronze layer) |
| Scalability | Limited by local machine | Auto-scales with Glue |

---

## Author

**Sneha Singh**
[LinkedIn](https://www.linkedin.com/in/sneha-singh-04a1a6254/) • [GitHub](https://github.com/singhsneha10)