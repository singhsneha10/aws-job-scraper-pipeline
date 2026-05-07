import sys
import json
import boto3
import requests
from datetime import datetime
from awsglue.utils import getResolvedOptions

# get job parameters passed in by glue
args = getResolvedOptions(sys.argv,['S3_BUCKET'])
S3_BUCKET = args['S3_BUCKET']

# step 1: fetch jobs from himalayas api
def fetch_jobs():
    print("Fetching jobs from HImalayas API...")
    url = "https://himalayas.app/jobs/api?q=data+engineer&limit=100"

    try:
        response = requests.geturl(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        jobs = data.get("jobs",[])
        print(f"Fetched {len(jobs)} jobs successfully.")
        return jobs
    except requests.exceptions.RequestExceptions as e:
        print(f"Failed to fetch jobs: {e}")
        raise

# step 2: clean and standardize each job records
def  transform_job(job):
    title = job.get("title","").strip()
    company = job.get("companyName","").strip()
    location = job.get("locationRestrictions",["Remote"])
    location = ", ".join(location) if isinstance(location, list) else location

    minSal = job.get("minSalary")
    maxSal = job.get("maxSalary")
    currency  = job.get("currency","USD")
    salary = f"{currency} {minSal} - {maxSal}" if minSal and maxSal else "Not Disclosed"

    job_url = job.get("applicationLink","").strip()
    job_type = job.get("jobType","Not Listed").strip()
    scraped_at = datetime.utcnow().isoformat()

    # skip incomplete records
    if not title or not company or not job_url:
        return None
    return {
        "title": title,
        "company": company,
        "location": location,
        "salary": salary,
        "job_url": job_url,
        "job_type": job_type,
        "source": "Himalayas",
        "scraped_at": scraped_at
    }

# step 3: save cleaned data to s3 as json
def save_to_s3(records):
    s3 = boto3.client("s3")

    # partition by date so files are organized by run date
    today = datetime.utcnow().strftime("%Y/%m/%d")
    timestamp = datetime.utcnow().strftime("H%M%S")
    key = f"bronze/job_listings/{today}/jobs_{timestamp}.json"

    # each line is one json record -- standard format for athena/glue
    body = "\n".join(json.dumps(record) for record in records)

    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"), ContentType="application/json")
    print(f"Saved {len(records)} records to s3://{S3_BUCKET}/{key}")

    # main
def main():
    raw_jobs = fetch_jobs()
    cleaned_jobs = [transform_job(j) for j in raw_jobs]
    valid_jobs = [j for j in cleaned_jobs if j is not None]

    print(f"Valid records after cleaning: {len(valid_jobs)}")
    save_to_s3(valid_jobs)
main()