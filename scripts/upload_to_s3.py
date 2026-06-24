"""Upload the sample Parquet file to the local S3 bucket."""

import os

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
S3_BUCKET = os.getenv("S3_BUCKET", "poc-bucket")
LOCAL_FILE = "./data/input/custody_position.parquet"
S3_KEY = "input/custody_position.parquet"


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )

    try:
        s3.create_bucket(Bucket=S3_BUCKET)
        print(f"Bucket criado: {S3_BUCKET}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket ja existe: {S3_BUCKET}")
    except Exception as e:
        print(f"Aviso ao criar bucket: {e}")

    s3.upload_file(LOCAL_FILE, S3_BUCKET, S3_KEY)
    print(f"Arquivo enviado: s3://{S3_BUCKET}/{S3_KEY}")


if __name__ == "__main__":
    main()
