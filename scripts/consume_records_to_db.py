"""
Consome registros da SQS #2, valida e insere em lote no PostgreSQL.

Fluxo de producao:
  SQS #2 (registros) ──▶ ECS (este script) ──▶ PostgreSQL (staging + error)

O script:
  1. Recebe ate 10 mensagens da SQS #2 (long polling)
  2. Para cada mensagem: extrai o registro e valida
  3. Batch INSERT: validos → staging, invalidos → error table
  4. Deleta mensagens apos COMMIT bem-sucedido
  5. Se o processo morre antes de deletar, a msg volta pra fila em 30s

Idempotencia:
  - ON CONFLICT (source_file, row_number) DO NOTHING impede duplicatas
  - Se a mesma msg chegar 2x (SQS at-least-once), staging nao duplica

Uso:
  python scripts/consume_records_to_db.py
"""

import hashlib
import json
import os

import boto3
import psycopg2
from botocore.config import Config
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

# SQS #2
SQS_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
RECORD_QUEUE = os.getenv("SQS_RECORD_QUEUE", "poc-record-queue")
BATCH_SIZE = int(os.getenv("SQS_RECEIVE_BATCH_SIZE", "10"))
VISIBILITY_TIMEOUT = int(os.getenv("SQS_VISIBILITY_TIMEOUT", "30"))

# PostgreSQL
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "pocdb")
PG_USER = os.getenv("POSTGRES_USER", "pocuser")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pocpass")


def get_db_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


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


def main():
    print("=" * 60)
    print("CONSUMER SQS #2 (Registros) → PostgreSQL")
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
        print(f"[ERRO] Fila '{RECORD_QUEUE}' nao existe.")
        return

    print(f"  SQS #2: {record_queue_url}")
    print(f"  DB:     {PG_HOST}:{PG_PORT}/{PG_DB}\n")

    conn = get_db_conn()
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
            print("[SQS #2] Fila vazia. Consumidor encerrado.")
            break

        valid_rows = []
        invalid_rows = []
        receipts = []

        for msg in msgs:
            receipts.append(msg["ReceiptHandle"])
            try:
                body = json.loads(msg["Body"])
            except json.JSONDecodeError:
                invalid_rows.append((
                    "unknown", "unknown", -1,
                    json.dumps({"raw": msg["Body"]}), "JSON invalido",
                ))
                continue

            record = body.get("record", {})
            source_file = body.get("source_file", "unknown")
            row_number = body.get("row_number", -1)
            batch_id = body.get("batch_id", "unknown")

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

        # Batch INSERT validos (1 statement)
        if valid_rows:
            execute_values(cur, """
                INSERT INTO custody_position_staging
                    (batch_id, source_file, row_number, record_hash,
                     account_id, asset_id, reference_date, quantity, amount)
                VALUES %s
                ON CONFLICT (source_file, row_number) DO NOTHING
            """, valid_rows)

        # Batch INSERT invalidos (1 statement)
        if invalid_rows:
            execute_values(cur, """
                INSERT INTO custody_position_error
                    (batch_id, source_file, row_number, payload, error_reason)
                VALUES %s
                ON CONFLICT (source_file, row_number) DO NOTHING
            """, invalid_rows)

        conn.commit()

        # Deleta mensagens da SQS apos COMMIT
        for receipt in receipts:
            sqs.delete_message(QueueUrl=record_queue_url, ReceiptHandle=receipt)

        total_recebidas += len(msgs)
        total_validas += len(valid_rows)
        total_invalidas += len(invalid_rows)
        total_deletadas += len(receipts)

        print(f"  Lote: {len(msgs)} msgs | staging: {len(valid_rows)} | erro: {len(invalid_rows)}")

    cur.close()
    conn.close()

    print(f"\n=== RESUMO SQS #2 → DB ===")
    print(f"  Recebidas:  {total_recebidas}")
    print(f"  Validas:    {total_validas}")
    print(f"  Invalidas:  {total_invalidas}")
    print(f"  Deletadas:  {total_deletadas}")


if __name__ == "__main__":
    main()
