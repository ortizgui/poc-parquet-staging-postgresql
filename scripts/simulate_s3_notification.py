"""
Simulates an S3 event notification.

Two modes:
  --mode sns  -> publishes to SNS topic (requires SNS subscription to SQS)
  --mode sqs  -> publishes directly to SQS queue (skipping SNS)

In production, S3 publishes directly via Bucket Notification Configuration.
Here in LocalStack, we can either use SNS (manual trigger) or SQS (direct).

The message format mimics S3 Event Notification:
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
  python scripts/simulate_s3_notification.py --bucket poc-bucket --key input/test.parquet
  python scripts/simulate_s3_notification.py --bucket poc-bucket --key input/test.parquet --mode sqs
"""

import argparse
import json
import os

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

SNS_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
SQS_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
SNS_TOPIC_NAME = os.getenv("SNS_TOPIC_NAME", "poc-notification-topic")
SQS_NOTIFICATION_QUEUE = os.getenv("SQS_NOTIFICATION_QUEUE", "poc-notification-queue")


def publish_sns(bucket: str, key: str, topic_name: str):
    sns = boto3.client(
        "sns",
        endpoint_url=SNS_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    topic_resp = sns.create_topic(Name=topic_name)
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
                        "name": bucket,
                    },
                    "object": {
                        "key": key,
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

    print(f"[NOTIFICATION] S3 Event published via SNS")
    print(f"  Topic ARN:  {topic_arn}")
    print(f"  Topic Name: {topic_name}")
    print(f"  Bucket:     {bucket}")
    print(f"  Key:        {key}")


def publish_sqs(bucket: str, key: str, queue_name: str):
    sqs = boto3.client(
        "sqs",
        endpoint_url=SQS_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )

    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]

    notification = {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "bucket": {
                        "name": bucket,
                    },
                    "object": {
                        "key": key,
                    },
                },
            }
        ]
    }

    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(notification),
    )

    print(f"[NOTIFICATION] S3 Event published directly to SQS")
    print(f"  Queue Name: {queue_name}")
    print(f"  Queue URL:  {queue_url}")
    print(f"  Bucket:     {bucket}")
    print(f"  Key:        {key}")


def main():
    parser = argparse.ArgumentParser(description="Simula S3 Event Notification")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--key", required=True, help="S3 object key")
    parser.add_argument("--mode", choices=["sns", "sqs"], default="sns",
                        help="Publish mode: sns or sqs (default: sns)")
    parser.add_argument("--topic", default=SNS_TOPIC_NAME, help="SNS topic name (sns mode)")
    parser.add_argument("--queue-name", default=SQS_NOTIFICATION_QUEUE,
                        help="SQS queue name (sqs mode, default: env SQS_NOTIFICATION_QUEUE)")
    args = parser.parse_args()

    if args.mode == "sqs":
        publish_sqs(args.bucket, args.key, args.queue_name)
    else:
        publish_sns(args.bucket, args.key, args.topic)


if __name__ == "__main__":
    main()
