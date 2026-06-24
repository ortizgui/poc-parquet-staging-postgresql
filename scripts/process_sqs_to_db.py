"""
Le mensagens de uma fila SQS, valida cada registro e insere em lote no PostgreSQL.

Fluxo:
  1. Recebe ate 10 mensagens do SQS (long polling)
  2. Para cada mensagem: valida o registro
  3. Insere em batch os validos → staging, invalidos → error table
  4. Deleta as mensagens processadas do SQS

Idempotencia:
  - Se a mesma mensagem chegar 2x (at-least-once), ON CONFLICT DO NOTHING
    na staging impede duplicacao
  - Se o consumer morre antes de deletar, a mensagem volta para a fila
    e e reprocessada (tambem protegido por ON CONFLICT)

Dependencias: boto3, psycopg2-binary, python-dotenv
"""

import hashlib
import json
import os
import uuid

import boto3
import psycopg2
from botocore.config import Config
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

# SQS
SQS_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
SQS_QUEUE_URL = os.getenv(
    "SQS_QUEUE_URL",
    "http://localhost:4566/000000000000/poc-queue",
)
SQS_BATCH_SIZE = int(os.getenv("SQS_RECEIVE_BATCH_SIZE", "10"))

# PostgreSQL
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "pocdb")
PG_USER = os.getenv("POSTGRES_USER", "pocuser")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pocpass")

# SQS Visibility timeout (segundos para processar antes da msg voltar pra fila)
VISIBILITY_TIMEOUT = 30


def get_db_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


def compute_record_hash(record: dict) -> str:
    raw = (
        f"{record['account_id']}|{record['asset_id']}|"
        f"{record['reference_date']}|{record['quantity']}|{record['amount']}"
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def validate(record: dict) -> list[str]:
    errors: list[str] = []
    acc_id = str(record.get("account_id", "")).strip()
    asset_id = str(record.get("asset_id", "")).strip()
    ref_date = record.get("reference_date")
    qty = record.get("quantity")
    amt = record.get("amount")

    if not acc_id:
        errors.append("account_id is empty")
    if not asset_id:
        errors.append("asset_id is empty")
    if not ref_date:
        errors.append("reference_date is null")
    if qty is None or (isinstance(qty, (int, float)) and qty < 0):
        errors.append(f"quantity is invalid: {qty}")
    if amt is None or (isinstance(amt, (int, float)) and amt < 0):
        errors.append(f"amount is invalid: {amt}")
    return errors


def main():
    print(f"Iniciando consumer SQS → PostgreSQL")
    print(f"Fila: {SQS_QUEUE_URL}")
    print(f"Batch size: {SQS_BATCH_SIZE}")

    # -------------------------------------------------------------------
    # Cliente SQS
    # -------------------------------------------------------------------
    sqs = boto3.client(
        "sqs",
        endpoint_url=SQS_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )

    conn = get_db_conn()
    cur = conn.cursor()

    total_recebidas = 0
    total_validas = 0
    total_invalidas = 0
    total_deletadas = 0

    # -------------------------------------------------------------------
    # Loop de consumo (polling)
    # -------------------------------------------------------------------
    while True:
        resp = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=SQS_BATCH_SIZE,
            VisibilityTimeout=VISIBILITY_TIMEOUT,
            WaitTimeSeconds=5,  # long polling — espera ate 5s se fila vazia
        )

        messages = resp.get("Messages", [])
        if not messages:
            print("Nenhuma mensagem na fila. Consumidor encerrado.")
            break

        valid_rows: list[tuple] = []
        invalid_rows: list[tuple] = []
        receipts: list[str] = []

        for msg in messages:
            receipt_handle = msg["ReceiptHandle"]
            receipts.append(receipt_handle)

            try:
                body = json.loads(msg["Body"])
            except json.JSONDecodeError as e:
                # Mensagem mal-formada — vai para erro sem registro
                invalid_rows.append((
                    str(uuid.uuid4()), "unknown", -1,
                    json.dumps({"raw": msg["Body"]}), f"JSON invalido: {e}",
                ))
                total_invalidas += 1
                continue

            record = body.get("record", {})
            source_file = body.get("source_file", "unknown")
            row_number = body.get("row_number", -1)
            batch_id = body.get("batch_id", str(uuid.uuid4()))

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
                total_invalidas += 1
            else:
                record_hash = compute_record_hash(record)
                valid_rows.append((
                    str(batch_id), source_file, row_number, record_hash,
                    record["account_id"], record["asset_id"],
                    record["reference_date"],
                    record["quantity"], record["amount"],
                ))

        # -------------------------------------------------------------------
        # Batch INSERT validos
        # -------------------------------------------------------------------
        if valid_rows:
            execute_values(cur, """
                INSERT INTO custody_position_staging
                    (batch_id, source_file, row_number, record_hash,
                     account_id, asset_id, reference_date, quantity, amount)
                VALUES %s
                ON CONFLICT (source_file, row_number) DO NOTHING
            """, valid_rows)
            # execute_values pode nao setar rowcount corretamente
            # com ON CONFLICT; usamos len() como contagem real
            total_validas += len(valid_rows)

        # -------------------------------------------------------------------
        # Batch INSERT invalidos
        # -------------------------------------------------------------------
        if invalid_rows:
            execute_values(cur, """
                INSERT INTO custody_position_error
                    (batch_id, source_file, row_number, payload, error_reason)
                VALUES %s
                ON CONFLICT (source_file, row_number) DO NOTHING
            """, invalid_rows)

        # -------------------------------------------------------------------
        # COMMIT + Deleta mensagens do SQS
        # -------------------------------------------------------------------
        conn.commit()

        for receipt in receipts:
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt)

        total_recebidas += len(messages)
        total_deletadas += len(receipts)

        print(f"  Lote: {len(messages)} msgs, "
              f"{len(valid_rows)} staging, "
              f"{len(invalid_rows)} erros")

    # -------------------------------------------------------------------
    # Fim
    # -------------------------------------------------------------------
    cur.close()
    conn.close()

    print(f"\nResumo:")
    print(f"  recebidas:  {total_recebidas}")
    print(f"  validas:    {total_validas}")
    print(f"  invalidas:  {total_invalidas}")
    print(f"  deletadas:  {total_deletadas}")


if __name__ == "__main__":
    main()
