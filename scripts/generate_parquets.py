"""Generate multiple Parquet files and upload to S3.

Usage:
    python3 scripts/generate_parquets.py --count 10 --records-per-file 5000
"""

import argparse
import os
import uuid
import random
from datetime import datetime, timedelta

import pandas as pd
import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
S3_BUCKET = os.getenv("S3_BUCKET", "poc-bucket")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "test")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

ACCOUNTS = [f"ACC{i:03d}" for i in range(1, 10001)]
ASSETS = ["PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3", "PERM4", "RENT3", "RADL3", "HAPV3", "WEGE3",
          "CCRO3", "EMBR3", "GGBR4", "CSNA3", "USIM5", "GOAU4", "BRAP4", "VALE5", "FIBR3", "CPFE3"]


def generate_records(count, days_back=30):
    """Generate random custody position records."""
    records = []
    base_date = datetime.now().date()
    dates = [base_date - timedelta(days=i) for i in range(days_back + 1)]
    
    for _ in range(count):
        account_id = random.choice(ACCOUNTS)
        asset_id = random.choice(ASSETS)
        reference_date = random.choice(dates)
        quantity = round(random.uniform(10, 10000), 4)
        amount = round(random.uniform(100, 1000000), 2)
        records.append({
            "account_id": account_id,
            "asset_id": asset_id,
            "reference_date": reference_date,
            "quantity": quantity,
            "amount": amount
        })
    
    return records


def upload_to_s3(parquet_path, s3_key):
    """Upload a parquet file to S3."""
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    s3_client.upload_file(parquet_path, S3_BUCKET, s3_key)
    print(f"  Uploaded: s3://{S3_BUCKET}/{s3_key}")
    return f"s3://{S3_BUCKET}/{s3_key}"


def main():
    parser = argparse.ArgumentParser(description="Generate multiple parquet files and upload to S3")
    parser.add_argument("--count", type=int, default=10,
                        help="Number of parquet files to generate")
    parser.add_argument("--records-per-file", type=int, default=5000,
                        help="Number of records per file")
    parser.add_argument("--prefix", type=str, default="input/",
                        help="S3 key prefix")
    args = parser.parse_args()
    
    print(f"Gerando {args.count} arquivos parquet com {args.records_per_file} registros cada...")
    
    s3_urls = []
    local_files = []
    
    for i in range(args.count):
        # Generate filename
        file_id = uuid.uuid4().hex[:8]
        filename = f"custody_{file_id}.parquet"
        local_path = f"/tmp/{filename}"
        s3_key = f"{args.prefix}{filename}"
        
        # Generate records
        records = generate_records(args.records_per_file)
        df = pd.DataFrame(records)
        df["reference_date"] = pd.to_datetime(df["reference_date"])
        
        # Save locally
        df.to_parquet(local_path, engine="pyarrow", index=False)
        local_files.append(local_path)
        
        # Upload to S3
        s3_url = upload_to_s3(local_path, s3_key)
        s3_urls.append(s3_url)
        
        print(f"  [{i+1}/{args.count}] {filename}: {len(records)} registros")
    
    print(f"\nTotal: {args.count} arquivos, {args.count * args.records_per_file} registros")
    print(f"Capacidade total de ingestão: ~{args.count * args.records_per_file} registros")
    
    # Cleanup local files
    for f in local_files:
        os.remove(f)
    
    return s3_urls


if __name__ == "__main__":
    main()
