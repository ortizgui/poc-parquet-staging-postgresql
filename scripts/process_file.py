"""Download a Parquet file from S3 and stage its records into PostgreSQL in chunks."""

import argparse
import hashlib
import io
import json
import os
import uuid

import boto3
import pandas as pd
import psycopg2
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "pocdb")
PG_USER = os.getenv("POSTGRES_USER", "pocuser")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pocpass")


def get_db_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def compute_record_hash(row):
    raw = f"{row['account_id']}|{row['asset_id']}|{row['reference_date']}|{row['quantity']}|{row['amount']}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def validate_row(row):
    errors = []
    acc_id = str(row.get("account_id", "")).strip()
    asset_id = str(row.get("asset_id", "")).strip()
    ref_date = row.get("reference_date")
    qty = row.get("quantity")
    amt = row.get("amount")

    if not acc_id:
        errors.append("account_id is empty")
    if not asset_id:
        errors.append("asset_id is empty")
    if ref_date is None or pd.isna(ref_date):
        errors.append("reference_date is null")
    if qty is None or pd.isna(qty) or qty < 0:
        errors.append(f"quantity is invalid: {qty}")
    if amt is None or pd.isna(amt) or amt < 0:
        errors.append(f"amount is invalid: {amt}")

    return errors


def insert_error(cur, batch_id, source_file, row_number, payload, reason):
    cur.execute(
        """
        INSERT INTO custody_position_error (batch_id, source_file, row_number, payload, error_reason)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (str(batch_id), source_file, row_number, json.dumps(payload), reason),
    )


def insert_staging(cur, batch_id, source_file, row_number, record_hash, row):
    cur.execute(
        """
        INSERT INTO custody_position_staging
            (batch_id, source_file, row_number, record_hash, account_id, asset_id, reference_date, quantity, amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_file, row_number) DO NOTHING
        """,
        (
            str(batch_id),
            source_file,
            row_number,
            record_hash,
            row["account_id"],
            row["asset_id"],
            row["reference_date"],
            row["quantity"],
            row["amount"],
        ),
    )
    return cur.rowcount


def main():
    parser = argparse.ArgumentParser(description="Process a Parquet file from S3 into staging")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--key", required=True, help="S3 object key")
    parser.add_argument("--chunk-size", type=int, default=5, help="Rows per transaction chunk")
    args = parser.parse_args()

    batch_id = uuid.uuid4()
    source_file = f"s3://{args.bucket}/{args.key}"

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )

    print("Baixando arquivo do S3...")
    resp = s3.get_object(Bucket=args.bucket, Key=args.key)
    df = pd.read_parquet(io.BytesIO(resp["Body"].read()), engine="pyarrow")

    total = len(df)
    valid_count = 0
    invalid_count = 0
    duplicate_skipped = 0
    chunk_count = 0

    conn = get_db_conn()

    for chunk_start in range(0, total, args.chunk_size):
        chunk_end = min(chunk_start + args.chunk_size, total)
        chunk = df.iloc[chunk_start:chunk_end]
        chunk_count += 1

        try:
            cur = conn.cursor()
            for idx, (_, row) in enumerate(chunk.iterrows()):
                row_number = chunk_start + idx
                errors = validate_row(row)
                payload = {
                    "account_id": str(row.get("account_id", "")),
                    "asset_id": str(row.get("asset_id", "")),
                    "reference_date": str(row.get("reference_date", "")),
                    "quantity": row.get("quantity"),
                    "amount": row.get("amount"),
                }

                if errors:
                    reason = "; ".join(errors)
                    insert_error(cur, batch_id, source_file, row_number, payload, reason)
                    invalid_count += 1
                else:
                    record_hash = compute_record_hash(row)
                    affected = insert_staging(cur, batch_id, source_file, row_number, record_hash, row)
                    if affected > 0:
                        valid_count += 1
                    else:
                        duplicate_skipped += 1

            conn.commit()
            cur.close()
        except Exception as e:
            conn.rollback()
            print(f"Erro no chunk {chunk_start}-{chunk_end}: {e}")

    conn.close()

    print(f"batch_id: {batch_id}")
    print(f"total lido: {total}")
    print(f"total valido: {valid_count}")
    print(f"total invalido: {invalid_count}")
    print(f"total ignorado por duplicidade: {duplicate_skipped}")
    print(f"chunks processados: {chunk_count}")


if __name__ == "__main__":
    main()
