"""
Simula o S3 enviando uma notificacao de objeto criado para um topico SNS.

Em producao, o S3 publica automaticamente (S3 Event Notification) no SNS.
Aqui no LocalStack, publicamos manualmente no topico SNS com o formato
que o S3 usaria:

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
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

SNS_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
SNS_TOPIC_NAME = os.getenv("SNS_TOPIC_NAME", "poc-notification-topic")


def main():
    parser = argparse.ArgumentParser(description="Simula S3 Event Notification via SNS")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--key", required=True, help="S3 object key")
    parser.add_argument("--topic", default=SNS_TOPIC_NAME, help="SNS topic name")
    args = parser.parse_args()

    sns = boto3.client(
        "sns",
        endpoint_url=SNS_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    # Ensure topic exists
    topic_resp = sns.create_topic(Name=args.topic)
    topic_arn = topic_resp["TopicArn"]

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

    sns.publish(
        TopicArn=topic_arn,
        Message=json.dumps(notification),
        Subject="S3 Event Notification",
    )

    print(f"[NOTIFICATION] S3 Event publicado via SNS")
    print(f"  Topico ARN:  {topic_arn}")
    print(f"  Topico Name: {args.topic}")
    print(f"  Bucket:      {args.bucket}")
    print(f"  Key:         {args.key}")


if __name__ == "__main__":
    main()
