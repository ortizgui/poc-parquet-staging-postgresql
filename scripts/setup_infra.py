"""
Setup infraestrutura no LocalStack: S3 bucket, SQS queue + DLQ, S3 Event Notification.

In production mode (default): S3 -> SQS directly (no SNS)
With --sns flag: also creates SNS topic and subscription (for manual simulation)

Uso:
  python scripts/setup_infra.py
  python scripts/setup_infra.py --sns    # Also creates SNS topic
"""

import argparse
import json
import os

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
S3_BUCKET = os.getenv("S3_BUCKET", "poc-bucket")
SNS_TOPIC_NAME = os.getenv("SNS_TOPIC_NAME", "poc-notification-topic")

SQS_NOTIFICATION_QUEUE = os.getenv("SQS_NOTIFICATION_QUEUE", "poc-notification-queue")
SQS_NOTIFICATION_DLQ = os.getenv("SQS_NOTIFICATION_DLQ", "poc-notification-dlq")

REGION = "us-east-1"


def main():
    parser = argparse.ArgumentParser(description="Setup S3 + SQS + S3 Notification infra")
    parser.add_argument("--sns", action="store_true",
                        help="Also create SNS topic (for manual notification simulation)")
    args = parser.parse_args()

    s3 = boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=REGION,
        config=Config(signature_version="s3v4"),
    )
    sqs = boto3.client(
        "sqs",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=REGION,
        config=Config(signature_version="s3v4"),
    )

    # --- S3 bucket ---
    try:
        s3.create_bucket(Bucket=S3_BUCKET)
        print(f"[S3] Bucket criado: {S3_BUCKET}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"[S3] Bucket ja existe: {S3_BUCKET}")

    # --- DLQ: notification ---
    notif_dlq_resp = sqs.create_queue(
        QueueName=SQS_NOTIFICATION_DLQ,
        Attributes={
            "MessageRetentionPeriod": "86400",
        },
    )
    notif_dlq_url = notif_dlq_resp["QueueUrl"]
    notif_dlq_attrs = sqs.get_queue_attributes(QueueUrl=notif_dlq_url, AttributeNames=["QueueArn"])
    notif_dlq_arn = notif_dlq_attrs["Attributes"]["QueueArn"]
    print(f"[SQS] DLQ notificacao criada: {notif_dlq_url}")

    # --- Notification queue (Captura Carteira) ---
    notif_redrive = json.dumps({
        "deadLetterTargetArn": notif_dlq_arn,
        "maxReceiveCount": 3,
    })
    notif_resp = sqs.create_queue(
        QueueName=SQS_NOTIFICATION_QUEUE,
        Attributes={
            "RedrivePolicy": notif_redrive,
            "VisibilityTimeout": "30",
        },
    )
    notif_queue_url = notif_resp["QueueUrl"]
    notif_queue_attrs = sqs.get_queue_attributes(QueueUrl=notif_queue_url, AttributeNames=["QueueArn"])
    notif_queue_arn = notif_queue_attrs["Attributes"]["QueueArn"]
    print(f"[SQS] Fila Captura Carteira criada: {notif_queue_url}")

    # --- S3 Bucket Notification -> SQS ---
    s3.put_bucket_notification_configuration(
        Bucket=S3_BUCKET,
        NotificationConfiguration={
            'QueueConfigurations': [
                {
                    'QueueArn': notif_queue_arn,
                    'Events': ['s3:ObjectCreated:*'],
                    'Filter': {
                        'Key': {
                            'FilterRules': [
                                {'Name': 'suffix', 'Value': '.parquet'}
                            ]
                        }
                    }
                }
            ]
        }
    )
    print(f"S3 Bucket Notification -> SQS configurada: {notif_queue_arn}")

    # --- SQS queue policy to allow S3 to send messages ---
    policy = {
        "Version": "2012-10-17",
        "Id": "S3SendPolicy",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "SQS:SendMessage",
                "Resource": notif_queue_arn,
                "Condition": {
                    "ArnLike": {
                        "aws:SourceArn": f"arn:aws:s3:::{S3_BUCKET}"
                    }
                },
            }
        ],
    }
    sqs.set_queue_attributes(
        QueueUrl=notif_queue_url,
        Attributes={"Policy": json.dumps(policy)},
    )
    print("[SQS] Politica de acesso S3 configurada na fila")

    # --- SNS topic (optional, for manual simulation) ---
    if args.sns:
        sns = boto3.client(
            "sns",
            endpoint_url=AWS_ENDPOINT_URL,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name=REGION,
        )
        topic_resp = sns.create_topic(Name=SNS_TOPIC_NAME)
        topic_arn = topic_resp["TopicArn"]
        print(f"[SNS] Topico criado: {topic_arn}")

        sub_resp = sns.subscribe(
            TopicArn=topic_arn,
            Protocol="sqs",
            Endpoint=notif_queue_arn,
            Attributes={
                "RawMessageDelivery": "true",
            },
        )
        sub_arn = sub_resp["SubscriptionArn"]
        print(f"[SNS] Subscription criada: {sub_arn}")

        # Extend SQS policy to also allow SNS
        policy["Statement"].append({
            "Effect": "Allow",
            "Principal": "*",
            "Action": "SQS:SendMessage",
            "Resource": notif_queue_arn,
            "Condition": {
                "ArnEquals": {
                    "aws:SourceArn": topic_arn,
                }
            },
        })
        sqs.set_queue_attributes(
            QueueUrl=notif_queue_url,
            Attributes={"Policy": json.dumps(policy)},
        )
        print("[SQS] Politica SNS adicionada")

    # --- Summary ---
    print("\n" + "=" * 60)
    print("INFRAESTRUTURA CONFIGURADA")
    print("=" * 60)
    print(f"  S3 Bucket:                {S3_BUCKET}")
    print(f"  Notif DLQ URL:            {notif_dlq_url}")
    print(f"  Notif DLQ ARN:            {notif_dlq_arn}")
    print(f"  Notif Queue URL:          {notif_queue_url}")
    print(f"  Notif Queue ARN:          {notif_queue_arn}")
    print(f"  S3 Notification -> SQS:   enabled (.parquet files)")
    if args.sns:
        print(f"  SNS Topic ARN:            {topic_arn}")
        print(f"  SNS Subscription ARN:     {sub_arn}")


if __name__ == "__main__":
    main()
