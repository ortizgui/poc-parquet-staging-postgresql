"""Generate unique parquet test data WITHOUT duplicates.

Uses itertools.product to deterministically generate unique (account_id, asset_id, reference_date)
combinations across all files.

Usage:
    python3 scripts/generate_unique_test_data.py --files 3 --records-per-file 5000 --upload
"""

import argparse
import itertools
import os
import random
import uuid
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

USED_COMBINATIONS = set()

def get_all_combinations(days_back=30):
    """Get all possible (account_id, asset_id, reference_date) combinations."""
    base_date = datetime.now().date()
    dates = [base_date - timedelta(days=i) for i in range(days_back + 1)]
    return itertools.product(ACCOUNTS, ASSETS, dates)

def generate_unique_records(count, days_back=30):
    """Generate unique records without duplicates."""
    global USED_COMBINATIONS

    records = []
    base_date = datetime.now().date()
    dates = [base_date - timedelta(days=i) for i in range(days_back + 1)]

    all_combinations = itertools.product(ACCOUNTS, ASSETS, dates)
    available = [(acc, asset, date) for acc, asset, date in all_combinations
                 if (acc, asset, date) not in USED_COMBINATIONS]

    if len(available) < count:
        raise ValueError(f"Requested {count} unique records but only {len(available)} available")

    selected = available[:count]
    USED_COMBINATIONS.update(selected)

    for account_id, asset_id, reference_date in selected:
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
    parser = argparse.ArgumentParser(description="Generate unique parquet test data without duplicates")
    parser.add_argument("--files", type=int, default=3,
                        help="Number of parquet files to generate")
    parser.add_argument("--records-per-file", type=int, default=5000,
                        help="Number of records per file")
    parser.add_argument("--days", type=int, default=30,
                        help="Number of days for reference dates")
    parser.add_argument("--prefix", type=str, default="input/",
                        help="S3 key prefix")
    parser.add_argument("--upload", action="store_true",
                        help="Upload files to S3")
    parser.add_argument("--output-dir", type=str, default="data",
                        help="Output directory for control CSV")
    args = parser.parse_args()

    base_date = datetime.now().date()
    dates = [base_date - timedelta(days=i) for i in range(args.days + 1)]
    total_combinations = len(ACCOUNTS) * len(ASSETS) * len(dates)
    total_requested = args.files * args.records_per_file

    print(f"Generating {args.files} parquet files with {args.records_per_file} records each...")
    print(f"Total unique combinations available: {total_combinations:,}")
    print(f"Total records requested: {total_requested:,}")

    if total_requested > total_combinations:
        print(f"ERROR: Requested {total_requested} records but only {total_combinations:,} available")
        return

    s3_urls = []
    local_files = []
    control_data = []

    for i in range(args.files):
        file_id = uuid.uuid4().hex[:8]
        filename = f"custody_unique_{file_id}.parquet"
        local_path = f"/tmp/{filename}"
        s3_key = f"{args.prefix}{filename}"

        records = generate_unique_records(args.records_per_file, args.days)
        df = pd.DataFrame(records)
        df["reference_date"] = pd.to_datetime(df["reference_date"])

        for _, row in df.iterrows():
            control_data.append({
                "account_id": row["account_id"],
                "asset_id": row["asset_id"],
                "reference_date": row["reference_date"].date(),
                "quantity": row["quantity"],
                "amount": row["amount"],
                "source_file": filename
            })

        df.to_parquet(local_path, engine="pyarrow", index=False)
        local_files.append(local_path)

        if args.upload:
            s3_url = upload_to_s3(local_path, s3_key)
            s3_urls.append(s3_url)

        print(f"  [{i+1}/{args.files}] {filename}: {len(records)} records, "
              f"cumulative: {len(USED_COMBINATIONS):,} unique")

    os.makedirs(args.output_dir, exist_ok=True)
    control_csv_path = os.path.join(args.output_dir, "test_data_control.csv")
    control_df = pd.DataFrame(control_data)
    control_df.to_csv(control_csv_path, index=False)
    print(f"\nControl CSV saved to: {control_csv_path}")

    print(f"\nSummary:")
    print(f"  Total unique combinations: {total_combinations:,}")
    print(f"  Total records generated: {len(control_data):,}")
    print(f"  Remaining available: {total_combinations - len(control_data):,}")
    print(f"  Duplicates across files: 0")

    for f in local_files:
        os.remove(f)

    return s3_urls if args.upload else []

if __name__ == "__main__":
    main()
