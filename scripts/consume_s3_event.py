"""
Consome notificacoes S3 da SQS #1, le o Parquet e envia cada registro para a SQS #2.

Fluxo de producao:
  S3 ──(notification)──▶ SQS #1 ──▶ ECS (este script) ──▶ SQS #2 (registros)

O script:
  1. Le mensagens da SQS #1 (notificacao S3)
  2. Extrai bucket + key do evento
  3. Le o Parquet em streaming (s3fs + row groups)
  4. Envia cada registro para a SQS #2 (lotes de 10)
  5. Deleta a mensagem de notificacao apos processar o arquivo

Uso:
  python scripts/consume_s3_event.py
"""

import json
import os
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


def ensure_record_queue(sqs) -> str:
    """Cria a SQS #2 (registros) se nao existir."""
    try:
        resp = sqs.get_queue_url(QueueName=RECORD_QUEUE)
        return resp["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        resp = sqs.create_queue(QueueName=RECORD_QUEUE)
        print(f"[RECORD QUEUE] Criada: {resp['QueueUrl']}")
        return resp["QueueUrl"]


def send_record_batch(sqs, queue_url: str, messages: list[dict]):
    """Envia ate 10 registros para a SQS #2."""
    entries = [
        {"Id": str(i), "MessageBody": json.dumps(msg, default=str)}
        for i, msg in enumerate(messages)
    ]
    resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
    return len(resp.get("Successful", [])), len(resp.get("Failed", []))


def process_event(sqs, record_queue_url: str, body: dict) -> int:
    """Processa uma notificacao S3: le o Parquet e envia registros para SQS #2."""
    batch_id = uuid.uuid4()
    records = body.get("Records", [])
    total_enviados = 0

    for event in records:
        bucket = event.get("s3", {}).get("bucket", {}).get("name", "")
        key = event.get("s3", {}).get("object", {}).get("key", "")

        if not bucket or not key:
            print(f"  [SKIP] Evento sem bucket/key: {event}")
            continue

        source_file = f"s3://{bucket}/{key}"
        s3_path = f"s3://{bucket}/{key}"
        print(f"\n[PROCESS] batch_id={batch_id} arquivo={source_file}")

        # --- Leitura do Parquet (streaming) ---
        fs = s3fs.S3FileSystem(
            key="test", secret="test",
            client_kwargs={"endpoint_url": S3_ENDPOINT, "region_name": "us-east-1"},
        )

        with fs.open(s3_path, "rb") as f:
            pf = pq.ParquetFile(f)
            total_rows = pf.metadata.num_rows
            num_rg = pf.metadata.num_row_groups
            print(f"  Linhas: {total_rows} | Row groups: {num_rg}")

            row_number = 0
            batch_buffer = []

            for rg_idx in range(num_rg):
                table = pf.read_row_group(rg_idx)
                df = table.to_pandas()

                for _, row in df.iterrows():
                    msg = {
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

                print(f"    RG {rg_idx}: enviados para SQS #2")

    return total_enviados


def main():
    print("=" * 60)
    print("CONSUMER SQS #1 (Notificacao S3) → SQS #2 (Registros)")
    print("=" * 60)

    sqs = boto3.client(
        "sqs",
        endpoint_url=SQS_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )

    # Descobre URL das filas
    try:
        notif_url = sqs.get_queue_url(QueueName=NOTIFICATION_QUEUE)["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        print(f"[ERRO] Fila '{NOTIFICATION_QUEUE}' nao existe. Execute simulate_s3_notification.py primeiro.")
        return

    record_queue_url = ensure_record_queue(sqs)
    print(f"  SQS #1 (notificacao): {notif_url}")
    print(f"  SQS #2 (registros):   {record_queue_url}\n")

    total_eventos = 0
    total_registros = 0

    while True:
        resp = sqs.receive_message(
            QueueUrl=notif_url,
            MaxNumberOfMessages=1,  # 1 evento por vez (1 arquivo = N registros)
            VisibilityTimeout=VISIBILITY_TIMEOUT,
            WaitTimeSeconds=5,
        )

        messages = resp.get("Messages", [])
        if not messages:
            print("\n[SQS #1] Fila vazia. Consumidor encerrado.")
            break

        for msg in messages:
            receipt = msg["ReceiptHandle"]
            try:
                body = json.loads(msg["Body"])
                enviados = process_event(sqs, record_queue_url, body)
                total_eventos += 1
                total_registros += enviados

                sqs.delete_message(QueueUrl=notif_url, ReceiptHandle=receipt)
                print(f"  [OK] Notificacao processada. {enviados} registros enviados para SQS #2")

            except Exception as e:
                print(f"  [ERRO] Falha ao processar evento: {e}")

    print(f"\n=== RESUMO ===")
    print(f"  Eventos S3 processados: {total_eventos}")
    print(f"  Registros enviados p/ SQS #2: {total_registros}")


if __name__ == "__main__":
    main()
