"""
Simula o S3 enviando uma notificacao de objeto criado para uma fila SQS.

Em producao, isso e feito automaticamente pela AWS (S3 Event Notification).
Aqui no LocalStack, enviamos manualmente uma mensagem com o formato que
o S3 usaria:

  {
    "Records": [{
      "eventName": "ObjectCreated:Put",
      "s3": {
        "bucket": { "name": "poc-bucket" },
        "object": { "key": "input/custody_position.parquet" }
      }
    }]
  }

Uso:
  python scripts/simulate_s3_notification.py --bucket poc-bucket --key input/custody_position.parquet
"""

import argparse
import json
import os

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

SQS_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
QUEUE_NAME = os.getenv("SQS_NOTIFICATION_QUEUE", "poc-notification-queue")


def ensure_queue(sqs) -> str:
    try:
        resp = sqs.get_queue_url(QueueName=QUEUE_NAME)
        return resp["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        resp = sqs.create_queue(QueueName=QUEUE_NAME)
        print(f"[NOTIFICATION] Fila criada: {resp['QueueUrl']}")
        return resp["QueueUrl"]


def main():
    parser = argparse.ArgumentParser(description="Simula S3 Event Notification para o SQS")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--key", required=True)
    args = parser.parse_args()

    sqs = boto3.client(
        "sqs",
        endpoint_url=SQS_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )

    queue_url = ensure_queue(sqs)

    # Mensagem no formato S3 Event Notification
    notification = {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "bucket": {
                        "name": args.bucket,
                    },
                    "object": {
                        "key": args.key,
                    },
                },
            }
        ]
    }

    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(notification))
    print(f"[NOTIFICATION] S3 Event enviado para {QUEUE_NAME}")
    print(f"  Bucket: {args.bucket}")
    print(f"  Key:    {args.key}")
    print(f"  Fila:   {queue_url}")


if __name__ == "__main__":
    main()
