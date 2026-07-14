"""
ECS Service: Consume S3 event notifications from SQS, call process_file.py for bulk insert.

Flow (direct - production):
  S3 -> SQS (notification queue) -> this script -> process_file.py --target direct -> custody_position

Flow (staging - backward compat):
  SNS -> SQS (notification queue) -> this script -> process_file.py --target staging -> custody_position_staging

Features:
  - Multi-consumer support with --consumer-id
  - SNS envelope unwrapping (handles both SNS and direct SQS messages)
  - Configurable max-messages for test scenarios
  - SQS queue depth monitoring
  - Structured logging with [CONSUMER:{id}] prefix

Uso:
  python scripts/consume_s3_event.py
  python scripts/consume_s3_event.py --target direct --consumer-id 1 --max-messages 10
"""

import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
SQS_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")

NOTIFICATION_QUEUE = os.getenv("SQS_NOTIFICATION_QUEUE", "poc-notification-queue")
VISIBILITY_TIMEOUT = int(os.getenv("SQS_VISIBILITY_TIMEOUT", "30"))
DEFAULT_TARGET = os.getenv("CONSUMER_TARGET", "direct")


def unwrap_message(body: dict) -> dict:
    """Unwrap SNS envelope if present, otherwise return body as-is."""
    if body.get("Type") == "Notification" and "Message" in body:
        try:
            inner = json.loads(body["Message"])
            return inner
        except (json.JSONDecodeError, TypeError):
            pass
    return body


def call_process_file(bucket: str, key: str, target: str) -> bool:
    """Call process_file.py to bulk insert Parquet data."""
    script_path = os.path.join(os.path.dirname(__file__), "process_file.py")

    cmd = [
        sys.executable,
        script_path,
        "--bucket", bucket,
        "--key", key,
        "--target", target,
    ]

    print(f"Calling process_file.py: bucket={bucket}, key={key}, target={target}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
        )

        if result.returncode == 0:
            print(f"process_file.py succeeded for s3://{bucket}/{key}")
            if result.stdout:
                for line in result.stdout.strip().split('\n'):
                    if line:
                        print(f"  {line}")
            return True
        else:
            print(f"process_file.py FAILED for s3://{bucket}/{key}")
            print(f"  stderr: {result.stderr[:500]}")
            return False

    except subprocess.TimeoutExpired:
        print(f"process_file.py TIMEOUT for s3://{bucket}/{key}")
        return False
    except Exception as e:
        print(f"Error calling process_file.py: {e}")
        return False


def process_event(body: dict, target: str) -> int:
    """Process a single S3 event notification."""
    records = body.get("Records", [])
    total_processed = 0

    for event in records:
        bucket = event.get("s3", {}).get("bucket", {}).get("name", "")
        key = event.get("s3", {}).get("object", {}).get("key", "")

        if not bucket or not key:
            print("[SKIP] Event missing bucket/key")
            continue

        if not key.endswith('.parquet'):
            print(f"[SKIP] Not a parquet file: {key}")
            continue

        source_file = f"s3://{bucket}/{key}"
        print(f"Processing: {source_file}")

        if call_process_file(bucket, key, target):
            total_processed += 1
        else:
            print(f"[ERROR] Failed to process: {source_file}")

    return total_processed


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Consume S3 event notifications from SQS")
    parser.add_argument("--consumer-id", default=None,
                        help="Consumer identifier (default: hostname)")
    parser.add_argument("--target", choices=["staging", "direct"],
                        default=DEFAULT_TARGET,
                        help=f"Insert target (default: {DEFAULT_TARGET})")
    parser.add_argument("--max-messages", type=int, default=0,
                        help="Max messages to process before exiting (0=unlimited)")
    parser.add_argument("--poll-wait", type=int, default=5,
                        help="SQS long-polling wait seconds (default: 5)")
    args = parser.parse_args()

    consumer_id = args.consumer_id or socket.gethostname()
    log_prefix = f"[CONSUMER:{consumer_id}]"

    print("=" * 60)
    print(f"{log_prefix} Target: {args.target}")
    if args.max_messages > 0:
        print(f"{log_prefix} Max messages: {args.max_messages}")
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
        print(f"{log_prefix} [ERROR] Queue '{NOTIFICATION_QUEUE}' not found. Run setup_infra.py first.")
        return

    print(f"{log_prefix} SQS notification queue: {notif_url}")
    print(f"{log_prefix} Poll wait: {args.poll_wait}s\n")

    total_eventos = 0
    total_arquivos = 0
    start_time = time.time()
    max_queue_depth = 0

    while True:
        if args.max_messages > 0 and total_eventos >= args.max_messages:
            print(f"{log_prefix} Reached max messages ({args.max_messages}), stopping.")
            break

        resp = sqs.receive_message(
            QueueUrl=notif_url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=VISIBILITY_TIMEOUT,
            WaitTimeSeconds=args.poll_wait,
        )

        messages = resp.get("Messages", [])
        if not messages:
            if total_eventos > 0:
                print(f"{log_prefix} Queue empty after {total_eventos} events. Consumer finishing.")
            else:
                elapsed = time.time() - start_time
                print(f"{log_prefix} No messages received in {elapsed:.1f}s. Exiting.")
            break

        for msg in messages:
            receipt = msg["ReceiptHandle"]
            try:
                raw_body = json.loads(msg["Body"])
                body = unwrap_message(raw_body)
                arquivos_processados = process_event(body, args.target)
                total_eventos += 1
                total_arquivos += arquivos_processados

                sqs.delete_message(QueueUrl=notif_url, ReceiptHandle=receipt)
                print(f"{log_prefix} [OK] S3 event processed, {arquivos_processados} file(s)")

            except Exception as e:
                print(f"{log_prefix} [ERROR] Failed to process event: {e}")

        # Query SQS queue depth
        try:
            attrs = sqs.get_queue_attributes(
                QueueUrl=notif_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            queue_depth = int(attrs['Attributes'].get('ApproximateNumberOfMessages', 0))
            if queue_depth > max_queue_depth:
                max_queue_depth = queue_depth
            print(f"{log_prefix} Queue depth: {queue_depth}")
        except Exception:
            pass

    elapsed = time.time() - start_time
    print(f"\n=== {log_prefix} SUMMARY ===")
    print(f"  Consumer ID:    {consumer_id}")
    print(f"  Target:         {args.target}")
    print(f"  S3 events:      {total_eventos}")
    print(f"  Files processed:{total_arquivos}")
    print(f"  Max queue depth:{max_queue_depth}")
    print(f"  Elapsed:        {elapsed:.1f}s")


if __name__ == "__main__":
    main()
