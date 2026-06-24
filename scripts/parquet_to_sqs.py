"""
Le um arquivo Parquet do S3 e envia cada registro como mensagem para uma fila SQS.

Fluxo:
  1. Conecta no S3 (Range GET streaming) e le os row groups
  2. Para cada linha, monta uma mensagem JSON
  3. Envia em lotes de 10 para o SQS (maximo permitido por batch)
  4. Nao valida — validacao ocorre no consumer (process_sqs_to_db.py)

Dependencias: boto3, pyarrow, pandas, s3fs, python-dotenv
"""

import argparse
import json
import os
import uuid

import boto3
import pandas as pd
import pyarrow.parquet as pq
import s3fs
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
SQS_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "poc-queue")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", f"http://localhost:4566/000000000000/{SQS_QUEUE_NAME}")


def create_queue_if_not_exists(sqs) -> str:
    """Cria a fila SQS no LocalStack se nao existir. Retorna a URL."""
    try:
        resp = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)
        return resp["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        resp = sqs.create_queue(QueueName=SQS_QUEUE_NAME)
        print(f"Fila SQS criada: {resp['QueueUrl']}")
        return resp["QueueUrl"]


def send_messages_batch(sqs, queue_url: str, messages: list[dict]):
    """Envia ate 10 mensagens para o SQS em um unico lote."""
    entries = []
    for i, msg in enumerate(messages):
        entries.append({
            "Id": str(i),
            "MessageBody": json.dumps(msg, default=str),
        })

    resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
    sucesso = len(resp.get("Successful", []))
    falha = len(resp.get("Failed", []))
    return sucesso, falha


def main():
    parser = argparse.ArgumentParser(
        description="Le um Parquet do S3 e envia cada registro para uma fila SQS"
    )
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--key", required=True)
    args = parser.parse_args()

    batch_id = uuid.uuid4()
    source_file = f"s3://{args.bucket}/{args.key}"
    s3_path = f"s3://{args.bucket}/{args.key}"

    print(f"batch_id: {batch_id}")
    print(f"Arquivo: {source_file}")
    print(f"SQS Queue: {SQS_QUEUE_URL}")

    # -------------------------------------------------------------------
    # S3 clients
    # -------------------------------------------------------------------
    fs = s3fs.S3FileSystem(
        key="test", secret="test",
        client_kwargs={"endpoint_url": S3_ENDPOINT, "region_name": "us-east-1"},
    )
    sqs = boto3.client(
        "sqs",
        endpoint_url=SQS_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )

    queue_url = create_queue_if_not_exists(sqs)

    # -------------------------------------------------------------------
    # Leitura do Parquet (streaming via s3fs + row groups)
    # -------------------------------------------------------------------
    with fs.open(s3_path, "rb") as f:
        pf = pq.ParquetFile(f)
        total_rows = pf.metadata.num_rows
        num_row_groups = pf.metadata.num_row_groups
        print(f"Total linhas: {total_rows} | Row groups: {num_row_groups}")

        enviadas = 0
        batch_buffer: list[dict] = []
        row_number = 0

        for rg_idx in range(num_row_groups):
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

                # SQS send_message_batch aceita ate 10 por vez
                if len(batch_buffer) == 10:
                    ok, fail = send_messages_batch(sqs, queue_url, batch_buffer)
                    enviadas += ok
                    batch_buffer.clear()

            # Final do row group: envia o que sobrou
            if batch_buffer:
                ok, fail = send_messages_batch(sqs, queue_url, batch_buffer)
                enviadas += ok
                batch_buffer.clear()

            print(f"  RG {rg_idx}: {len(df)} linhas enviadas para SQS")

    print(f"\nResumo:")
    print(f"  batch_id:    {batch_id}")
    print(f"  total lido:  {total_rows}")
    print(f"  enviadas:    {enviadas}")
    print(f"  fila SQS:    {queue_url}")


if __name__ == "__main__":
    main()
