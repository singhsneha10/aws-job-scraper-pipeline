/*
===============================================================================
Redshift DDL: Job Listings Table
===============================================================================
Purpose:
    Creates the destination table in Redshift that stores cleaned job listings
    scraped from the Himalayas API. Data is loaded from S3 via COPY command.

Run this once in Redshift Query Editor before running the Glue pipeline.
===============================================================================
*/

-- ============================================================
-- Create schema
-- ============================================================

CREATE SCHEMA IF NOT EXISTS job_listings;

-- ============================================================
-- job_listings table
-- ============================================================

DROP TABLE IF EXISTS jobs.job_listings;
CREATE TABLE jobs.job_listings (
    id BIGINT IDENTITY(1,1),      --autoincrementing surrogate key
    title VARCHAR(255) NOT NULL,
    company VARCHAR(255) NOT NULL,
    location VARCHAR(255) NOT NULL,
    salary VARCHAR(100),
    job_url VARCHAR(1000) NOT NULL,
    job_type VARCHAR(100),
    source VARCHAR(100),
    scraped_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT GETDATE()  --when it landed at redshift

--prevent duplicate url across loads
    UNIQUE(job_url)
)
DISTSTYLE AUTO
SORTKEY (scraped_at);