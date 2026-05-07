import sys
import boto3
import psycopg2
from awsglue.utils import getResolvedOptions
from datetime import datetime, timedelta

# ============================================================
# Get parameters passed in by Glue
# ============================================================

args = getResolvedOptions(sys.argv,[
    'S3_BUCKET',
    'REDSHIFT_HOST',
    'REDSHIFT_DB',
    'REDSHIFT_USER',
    'REDSHIFT_PASSWORD',
    'REDSHIFT_PORT'
])

S3_BUCKET = args['S3_BUCKET']
REDSHIFT_HOST = args['REDSHIFT_HOST']
REDSHIFT_DB = args['REDSHIFT_DB']
REDSHIFT_USER = args['REDSHIFT_USER']
REDSHIFT_PASSWORD = args['REDSHIFT_PASSWORD']
REDSHIFT_PORT = int(args['REDSHIFT_PORT'])

# step1: read today's json files from s3 (bronze layer)
def read_from_s3():
    s3 = boto3.client("s3")
    today = datetime.utcnow().strftime("%Y/%m/%d")
    prefix = f"bronze/jobs/{today}/"

    print(f"Reading files from s3://{S3_BUCKET}/{prefix}")

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    files = response.get("Contents",[])

    if not files:
        print("No files found for today. Exiting.")
        return []
    all_records = []
    for file in files:
        key = file["Key"]
        print(f"Processing file: {key}")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = obj["Body"].read().decode("utf-8")

        # each line is one json record
        for line in body.strip().split("\n"):
            if line:
                all_records.append(json.loads(line))

    print(f"Read {len(all_records)} records from s3.")
    return all_records

# step 2: connect to redshift
def get_redshift_connection():
    try:
        conn = psycopg2.connect(
            host=REDSHIFT_HOST,
            dbname=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD,
            port=REDSHIFT_PORT
        )
        print("Connected to Redshift successfully.")
        return conn
    except Exception as e:
        print(f"Failed to connect to Redshift: {e}")
        raise

# step 3: insert records into redshift idempotent
def load_to_redshift(records):
    if not records:
        print("No records to load. Exiting.")
        return
    
    conn = get_redshift_connection()
    cursor = conn.cursor()

    inserted = 0
    skipped = 0
    for record in records:
        try:
            cursor.execute("""
                INSERT INTO jobs.job_listings (title, company, location, salary, job_url, job_type, source, scraped_at)
                SELECT %s, %s, %s, %s, %s, %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM jobs.job_listings WHERE job_url = %s
                );
            """, (record["title"], 
                  record["company"], 
                  record["location"], 
                  record["salary"], 
                  record["job_url"], 
                  record["job_type"], 
                  record["source"], 
                  record["scraped_at"], 
                  record["job_url"]  # for the WHERE NOT EXISTS check
                  )
                  )
            if cursor.rowcount > 0:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"Failed to insert record: {record.get('job_url')}: {e}")
            continue           
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Load complete: inserted {inserted} skipped {skipped} duplicates.")

# ============================================================
# Main
# ============================================================

def main():
    records = read_from_s3()
    load_to_redshift(records)
main()