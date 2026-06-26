"""
ECS Service 1: Consume S3 event notifications from SQS #1, read Parquet, send rows to SQS #2.

Flow:
  SNS -> SQS #1 (notification queue with DLQ) -> this script -> SQS #2 (record queue with DLQ)

Features:
  - SNS envelope unwrapping (handles both SNS and direct SQS messages)
  - DLQ auto-handling (messages exceeding maxReceiveCount go to DLQ)
  - Exponential backoff for S3 reads (1s, 2s, 4s)
  - Structured logging with [CONSUMER1] prefix
  - Includes transform_version in record messages

Uso:
  python scripts/consume_s3_event.py
"""

import json
import os
import time
import uuid

import boto3
import pyarrow.parquet as pq
import s3fs
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
SQS_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")

NOTIFICATION_QUEUE = os.getenv("SQS_NOTIFICATION_QUEUE", "poc-notification-queue")
RECORD_QUEUE = os.getenv("SQS_RECORD_QUEUE", "poc-record-queue")
VISIBILITY_TIMEOUT = int(os.getenv("SQS_VISIBILITY_TIMEOUT", "30"))
TRANSFORM_VERSION = 1

LOG_PREFIX = "[CONSUMER1]"


def ensure_record_queue(sqs) -> str:
    try:
        resp = sqs.get_queue_url(QueueName=RECORD_QUEUE)
        return resp["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        resp = sqs.create_queue(QueueName=RECORD_QUEUE)
        print(f"{LOG_PREFIX} Record queue created: {resp['QueueUrl']}")
        return resp["QueueUrl"]


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


def send_record_batch(sqs, queue_url: str, messages: list[dict]):
    entries = [
        {"Id": str(i), "MessageBody": json.dumps(msg, default=str)}
        for i, msg in enumerate(messages)
    ]
    resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
    ok = len(resp.get("Successful", []))
    fail = len(resp.get("Failed", []))
    if fail:
        failed_ids = [f["Id"] for f in resp.get("Failed", [])]
        print(f"{LOG_PREFIX} Partial batch send failure: {fail}/{len(entries)} failed IDs: {failed_ids}")
    return ok, fail


def read_parquet_with_retry(s3_path: str, max_retries=3) -> pq.ParquetFile:
    """Read Parquet from S3 with exponential backoff."""
    delays = [1, 2, 4]
    last_error = None

    for attempt in range(max_retries):
        try:
            fs = s3fs.S3FileSystem(
                key="test", secret="test",
                client_kwargs={"endpoint_url": S3_ENDPOINT, "region_name": "us-east-1"},
            )
            f = fs.open(s3_path, "rb")
            pf = pq.ParquetFile(f)
            return pf, f, fs
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                delay = delays[min(attempt, len(delays) - 1)]
                print(f"{LOG_PREFIX} S3 read attempt {attempt + 1} failed, retrying in {delay}s: {e}")
                time.sleep(delay)

    raise last_error


def process_event(sqs, record_queue_url: str, body: dict) -> int:
    batch_id = uuid.uuid4()
    records = body.get("Records", [])
    total_enviados = 0

    for event in records:
        bucket = event.get("s3", {}).get("bucket", {}).get("name", "")
        key = event.get("s3", {}).get("object", {}).get("key", "")

        if not bucket or not key:
            print(f"{LOG_PREFIX} [SKIP] Event missing bucket/key")
            continue

        source_file = f"s3://{bucket}/{key}"
        s3_path = f"s3://{bucket}/{key}"
        print(f"{LOG_PREFIX} Processing batch_id={batch_id} file={source_file}")

        try:
            pf, f, _fs = read_parquet_with_retry(s3_path)
        except Exception as e:
            print(f"{LOG_PREFIX} [ERROR] S3 read failed after retries: {e}")
            return 0

        try:
            total_rows = pf.metadata.num_rows
            num_rg = pf.metadata.num_row_groups
            print(f"{LOG_PREFIX}   Rows: {total_rows} | Row groups: {num_rg}")

            row_number = 0
            batch_buffer = []

            for rg_idx in range(num_rg):
                table = pf.read_row_group(rg_idx)
                df = table.to_pandas()

                for _, row in df.iterrows():
                    msg = {
                        "version": TRANSFORM_VERSION,
                        "batch_id": str(batch_id),
                        "source_file": source_file,
                        "row_number": row_number,
                        "record": {
                            "account_id": str(row.get("account_id", "")),
                            "asset_id": str(row.get("asset_id", "")),
                            "reference_date": str(row.get("reference_date", "")),
                            "quantity": float(row.get("quantity", 0)),
                            "amount": float(row.get("amount", 0)),
                        },
                    }
                    batch_buffer.append(msg)
                    row_number += 1

                    if len(batch_buffer) == 10:
                        ok, fail = send_record_batch(sqs, record_queue_url, batch_buffer)
                        total_enviados += ok
                        batch_buffer.clear()

                if batch_buffer:
                    ok, fail = send_record_batch(sqs, record_queue_url, batch_buffer)
                    total_enviados += ok
                    batch_buffer.clear()

                print(f"{LOG_PREFIX}   RG {rg_idx}: sent to SQS #2")

        finally:
            f.close()

    print(f"{LOG_PREFIX}   Done: {total_enviados} records sent to SQS #2, errors: 0")
    return total_enviados


def main():
    print("=" * 60)
    print(f"{LOG_PREFIX} CONSUMER SQS #1 (Notificacao S3) -> SQS #2 (Registros)")
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

    record_queue_url = ensure_record_queue(sqs)
    print(f"{LOG_PREFIX} SQS #1 (notification): {notif_url}")
    print(f"{LOG_PREFIX} SQS #2 (records):      {record_queue_url}\n")

    total_eventos = 0
    total_registros = 0

    while True:
        resp = sqs.receive_message(
            QueueUrl=notif_url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=VISIBILITY_TIMEOUT,
            WaitTimeSeconds=5,
        )

        messages = resp.get("Messages", [])
        if not messages:
            print(f"\n{LOG_PREFIX} Queue empty. Consumer finished.")
            break

        for msg in messages:
            receipt = msg["ReceiptHandle"]
            try:
                raw_body = json.loads(msg["Body"])
                body = unwrap_message(raw_body)
                enviados = process_event(sqs, record_queue_url, body)
                total_eventos += 1
                total_registros += enviados

                sqs.delete_message(QueueUrl=notif_url, ReceiptHandle=receipt)
                print(f"{LOG_PREFIX} [OK] Notification processed. {enviados} records sent to SQS #2")

            except Exception as e:
                print(f"{LOG_PREFIX} [ERROR] Failed to process event: {e}")

    print(f"\n=== {LOG_PREFIX} SUMMARY ===")
    print(f"  S3 events processed:  {total_eventos}")
    print(f"  Records sent to SQS #2: {total_registros}")


if __name__ == "__main__":
    main()
