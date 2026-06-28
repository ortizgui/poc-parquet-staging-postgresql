"""
ECS Service: Consume S3 event notifications from SQS #1, call process_file.py for bulk insert.

Flow (Flow B - direct bulk insert):
  SNS -> SQS #1 (notification queue) -> this script -> process_file.py -> Staging Table

Features:
  - SNS envelope unwrapping (handles both SNS and direct SQS messages)
  - Calls process_file.py for each S3 event (bulk insert to staging)
  - Exponential backoff for S3 reads (1s, 2s, 4s) handled by process_file.py
  - Structured logging with [CONSUMER] prefix

Uso:
  python scripts/consume_s3_event.py
"""

import json
import os
import subprocess
import sys
import time

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
SQS_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")

NOTIFICATION_QUEUE = os.getenv("SQS_NOTIFICATION_QUEUE", "poc-notification-queue")
VISIBILITY_TIMEOUT = int(os.getenv("SQS_VISIBILITY_TIMEOUT", "30"))

LOG_PREFIX = "[CONSUMER]"


def unwrap_message(body: dict) -> dict:
    """Unwrap SNS envelope if present, otherwise return body as-is."""
    if body.get("Type") == "Notification" and "Message" in body:
        try:
            inner = json.loads(body["Message"])
            return inner
        except (json.JSONDecodeError, TypeError):
            pass
    else:
        print(f"{LOG_PREFIX} Non-SNS message format received (processing anyway)")
    return body


def call_process_file(bucket: str, key: str) -> bool:
    """Call process_file.py to bulk insert Parquet to staging."""
    script_path = os.path.join(os.path.dirname(__file__), "process_file.py")
    
    cmd = [
        sys.executable,  # Use current Python interpreter
        script_path,
        "--bucket", bucket,
        "--key", key,
    ]
    
    print(f"{LOG_PREFIX} Calling process_file.py: bucket={bucket}, key={key}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout per file
        )
        
        if result.returncode == 0:
            print(f"{LOG_PREFIX} process_file.py succeeded for s3://{bucket}/{key}")
            if result.stdout:
                for line in result.stdout.strip().split('\n'):
                    if line:
                        print(f"{LOG_PREFIX}   {line}")
            return True
        else:
            print(f"{LOG_PREFIX} process_file.py FAILED for s3://{bucket}/{key}")
            print(f"{LOG_PREFIX}   stderr: {result.stderr[:500]}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"{LOG_PREFIX} process_file.py TIMEOUT for s3://{bucket}/{key}")
        return False
    except Exception as e:
        print(f"{LOG_PREFIX} Error calling process_file.py: {e}")
        return False


def process_event(body: dict) -> int:
    """Process a single S3 event notification."""
    records = body.get("Records", [])
    total_processed = 0

    for event in records:
        bucket = event.get("s3", {}).get("bucket", {}).get("name", "")
        key = event.get("s3", {}).get("object", {}).get("key", "")

        if not bucket or not key:
            print(f"{LOG_PREFIX} [SKIP] Event missing bucket/key")
            continue

        # Skip non-parquet files
        if not key.endswith('.parquet'):
            print(f"{LOG_PREFIX} [SKIP] Not a parquet file: {key}")
            continue

        source_file = f"s3://{bucket}/{key}"
        print(f"{LOG_PREFIX} Processing: {source_file}")

        if call_process_file(bucket, key):
            total_processed += 1
        else:
            print(f"{LOG_PREFIX} [ERROR] Failed to process: {source_file}")

    return total_processed


def main():
    print("=" * 60)
    print(f"{LOG_PREFIX} CONSUMER - SNS/SQS -> process_file.py -> Staging")
    print("=" * 60)

    sqs = boto3.client(
        "sqs",
        endpoint_url=SQS_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )

    try:
        notif_url = sqs.get_queue_url(QueueName=NOTIFICATION_QUEUE)["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        print(f"{LOG_PREFIX} [ERROR] Queue '{NOTIFICATION_QUEUE}' not found. Run setup_infra.py first.")
        return

    print(f"{LOG_PREFIX} SQS notification queue: {notif_url}\n")

    total_eventos = 0
    total_arquivos = 0
    start_time = time.time()

    while True:
        resp = sqs.receive_message(
            QueueUrl=notif_url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=VISIBILITY_TIMEOUT,
            WaitTimeSeconds=5,
        )

        messages = resp.get("Messages", [])
        if not messages:
            elapsed = time.time() - start_time
            if total_eventos > 0:
                print(f"\n{LOG_PREFIX} Queue empty after {total_eventos} events. Consumer finishing.")
            else:
                print(f"\n{LOG_PREFIX} No messages received in {elapsed:.1f}s. Waiting...")
            break

        for msg in messages:
            receipt = msg["ReceiptHandle"]
            try:
                raw_body = json.loads(msg["Body"])
                body = unwrap_message(raw_body)
                arquivos_processados = process_event(body)
                total_eventos += 1
                total_arquivos += arquivos_processados

                sqs.delete_message(QueueUrl=notif_url, ReceiptHandle=receipt)
                print(f"{LOG_PREFIX} [OK] S3 event processed, {arquivos_processados} file(s) bulk-inserted")

            except Exception as e:
                print(f"{LOG_PREFIX} [ERROR] Failed to process event: {e}")

    elapsed = time.time() - start_time
    print(f"\n=== {LOG_PREFIX} SUMMARY ===")
    print(f"  S3 events processed: {total_eventos}")
    print(f"  Files bulk-inserted: {total_arquivos}")
    print(f"  Elapsed time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
