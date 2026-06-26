"""
ECS Service 2: Consume records from SQS #2, validate, and insert into buffer table.

Flow:
  SQS #2 (record queue with DLQ) -> this script -> custody_position_buffer + custody_position_error

Features:
  - SNS envelope unwrapping (handles both SNS and direct SQS messages)
  - DLQ auto-handling (messages exceeding maxReceiveCount go to DLQ)
  - Partial batch resilience: valid rows go to buffer, invalid to error table
  - Retry with backoff for DB connection failures (1s, 2s)
  - Structured logging with [CONSUMER2] prefix
  - Resilience: if any INSERT fails, the entire batch is ROLLBACKed and messages
    return to SQS via visibility timeout for reprocessing (all-or-nothing per
    batch with SQS retry). ON CONFLICT DO NOTHING ensures idempotency on retry.

Uso:
  python scripts/consume_records_to_db.py
"""

import hashlib
import json
import os
import time

import boto3
import psycopg2
from botocore.config import Config
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

SQS_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
RECORD_QUEUE = os.getenv("SQS_RECORD_QUEUE", "poc-record-queue")
BATCH_SIZE = int(os.getenv("SQS_RECEIVE_BATCH_SIZE", "10"))
VISIBILITY_TIMEOUT = int(os.getenv("SQS_VISIBILITY_TIMEOUT", "30"))

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "pocdb")
PG_USER = os.getenv("POSTGRES_USER", "pocuser")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pocpass")

LOG_PREFIX = "[CONSUMER2]"


def get_db_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


def get_db_conn_with_retry(max_retries=3):
    delays = [1, 2]
    for attempt in range(max_retries):
        try:
            return get_db_conn()
        except Exception as e:
            if attempt < max_retries - 1:
                delay = delays[min(attempt, len(delays) - 1)]
                print(f"{LOG_PREFIX} DB connection attempt {attempt + 1} failed, retrying in {delay}s: {e}")
                time.sleep(delay)
            else:
                raise


def compute_hash(record: dict) -> str:
    raw = (
        f"{record['account_id']}|{record['asset_id']}|"
        f"{record['reference_date']}|{record['quantity']}|{record['amount']}"
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def validate(record: dict) -> list[str]:
    errors = []
    acc = str(record.get("account_id", "")).strip()
    asset = str(record.get("asset_id", "")).strip()
    ref = record.get("reference_date")
    qty = record.get("quantity")
    amt = record.get("amount")

    if not acc:
        errors.append("account_id is empty")
    if not asset:
        errors.append("asset_id is empty")
    if not ref:
        errors.append("reference_date is null")
    if qty is None or (isinstance(qty, (int, float)) and qty < 0):
        errors.append(f"quantity is invalid: {qty}")
    if amt is None or (isinstance(amt, (int, float)) and amt < 0):
        errors.append(f"amount is invalid: {amt}")
    return errors


def unwrap_message(body: dict) -> dict:
    if body.get("Type") == "Notification" and "Message" in body:
        try:
            inner = json.loads(body["Message"])
            return inner
        except (json.JSONDecodeError, TypeError):
            pass
    else:
        print(f"{LOG_PREFIX} Non-SNS message format received (processing anyway)")
    return body


def main():
    print("=" * 60)
    print(f"{LOG_PREFIX} CONSUMER SQS #2 (Registros) -> PostgreSQL")
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
        record_queue_url = sqs.get_queue_url(QueueName=RECORD_QUEUE)["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        print(f"{LOG_PREFIX} [ERROR] Queue '{RECORD_QUEUE}' not found. Run setup_infra.py first.")
        return

    print(f"{LOG_PREFIX} SQS #2: {record_queue_url}")
    print(f"{LOG_PREFIX} DB:     {PG_HOST}:{PG_PORT}/{PG_DB}\n")

    try:
        conn = get_db_conn_with_retry()
    except Exception as e:
        print(f"{LOG_PREFIX} [ERROR] Failed to connect to DB: {e}")
        return

    cur = conn.cursor()

    total_recebidas = 0
    total_validas = 0
    total_invalidas = 0
    total_deletadas = 0

    while True:
        resp = sqs.receive_message(
            QueueUrl=record_queue_url,
            MaxNumberOfMessages=BATCH_SIZE,
            VisibilityTimeout=VISIBILITY_TIMEOUT,
            WaitTimeSeconds=5,
        )

        msgs = resp.get("Messages", [])
        if not msgs:
            print(f"{LOG_PREFIX} Queue empty. Consumer finished.")
            break

        valid_rows = []
        invalid_rows = []
        receipts = []

        for msg in msgs:
            receipts.append(msg["ReceiptHandle"])
            try:
                raw_body = json.loads(msg["Body"])
                body = unwrap_message(raw_body)
            except json.JSONDecodeError:
                invalid_rows.append((
                    "unknown", "unknown", -1,
                    json.dumps({"raw": msg["Body"]}), "Invalid JSON",
                ))
                continue

            record = body.get("record", {})
            source_file = body.get("source_file", "unknown")
            row_number = body.get("row_number", -1)
            batch_id = body.get("batch_id", "unknown")
            # version = body.get("version", None)  # available for future use

            errors = validate(record)
            payload = {
                "account_id": record.get("account_id", ""),
                "asset_id": record.get("asset_id", ""),
                "reference_date": record.get("reference_date", ""),
                "quantity": record.get("quantity"),
                "amount": record.get("amount"),
            }

            if errors:
                invalid_rows.append((
                    str(batch_id), source_file, row_number,
                    json.dumps(payload), "; ".join(errors),
                ))
            else:
                record_hash = compute_hash(record)
                valid_rows.append((
                    str(batch_id), source_file, row_number, record_hash,
                    record["account_id"], record["asset_id"],
                    record["reference_date"],
                    record["quantity"], record["amount"],
                ))

        # Batch INSERT to buffer table (valid records)
        if valid_rows:
            try:
                execute_values(cur, """
                    INSERT INTO custody_position_buffer
                        (batch_id, source_file, row_number, record_hash,
                         account_id, asset_id, reference_date, quantity, amount)
                    VALUES %s
                    ON CONFLICT (source_file, row_number) DO NOTHING
                """, valid_rows)
            except Exception as e:
                print(f"{LOG_PREFIX} [ERROR] Buffer INSERT failed: {e}")
                conn.rollback()
                cur.close()
                try:
                    conn = get_db_conn_with_retry()
                except Exception:
                    print(f"{LOG_PREFIX} [ERROR] DB reconnection failed, stopping")
                    return
                cur = conn.cursor()
                continue

        # Batch INSERT to error table (invalid records)
        if invalid_rows:
            try:
                execute_values(cur, """
                    INSERT INTO custody_position_error
                        (batch_id, source_file, row_number, payload, error_reason)
                    VALUES %s
                    ON CONFLICT (source_file, row_number) DO NOTHING
                """, invalid_rows)
            except Exception as e:
                print(f"{LOG_PREFIX} [ERROR] Error table INSERT failed: {e}")
                conn.rollback()
                cur.close()
                try:
                    conn = get_db_conn_with_retry()
                except Exception:
                    print(f"{LOG_PREFIX} [ERROR] DB reconnection failed, stopping")
                    return
                cur = conn.cursor()
                continue

        try:
            conn.commit()
        except Exception as e:
            print(f"{LOG_PREFIX} [ERROR] COMMIT failed: {e}")
            conn.rollback()
            cur.close()
            try:
                conn = get_db_conn_with_retry()
            except Exception:
                print(f"{LOG_PREFIX} [ERROR] DB reconnection failed, stopping")
                return
            cur = conn.cursor()
            continue

        # Delete messages from SQS after successful COMMIT
        for receipt in receipts:
            sqs.delete_message(QueueUrl=record_queue_url, ReceiptHandle=receipt)

        total_recebidas += len(msgs)
        total_validas += len(valid_rows)
        total_invalidas += len(invalid_rows)
        total_deletadas += len(receipts)

        print(f"{LOG_PREFIX} Batch: {len(msgs)} msgs | buffer: {len(valid_rows)} | error: {len(invalid_rows)}")

    cur.close()
    conn.close()

    print(f"\n=== {LOG_PREFIX} SUMMARY ===")
    print(f"  Received:   {total_recebidas}")
    print(f"  Valid:      {total_validas}")
    print(f"  Invalid:    {total_invalidas}")
    print(f"  Deleted:    {total_deletadas}")


if __name__ == "__main__":
    main()
