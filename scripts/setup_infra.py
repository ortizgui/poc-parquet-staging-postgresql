"""
Setup infraestrutura no LocalStack: S3 bucket, SNS topic, SQS queues + DLQs, subscriptions.

Uso:
  python scripts/setup_infra.py
"""

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
SQS_RECORD_QUEUE = os.getenv("SQS_RECORD_QUEUE", "poc-record-queue")
SQS_NOTIFICATION_DLQ = os.getenv("SQS_NOTIFICATION_DLQ", "poc-notification-dlq")
SQS_RECORD_DLQ = os.getenv("SQS_RECORD_DLQ", "poc-record-dlq")

REGION = "us-east-1"


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=REGION,
        config=Config(signature_version="s3v4"),
    )
    sns = boto3.client(
        "sns",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=REGION,
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

    # --- SNS topic ---
    topic_resp = sns.create_topic(Name=SNS_TOPIC_NAME)
    topic_arn = topic_resp["TopicArn"]
    print(f"[SNS] Topico criado: {topic_arn}")

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

    # --- Notification queue ---
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
    print(f"[SQS] Fila notificacao criada: {notif_queue_url}")

    # --- DLQ: records ---
    record_dlq_resp = sqs.create_queue(
        QueueName=SQS_RECORD_DLQ,
        Attributes={
            "MessageRetentionPeriod": "86400",
        },
    )
    record_dlq_url = record_dlq_resp["QueueUrl"]
    record_dlq_attrs = sqs.get_queue_attributes(QueueUrl=record_dlq_url, AttributeNames=["QueueArn"])
    record_dlq_arn = record_dlq_attrs["Attributes"]["QueueArn"]
    print(f"[SQS] DLQ registros criada: {record_dlq_url}")

    # --- Record queue ---
    record_redrive = json.dumps({
        "deadLetterTargetArn": record_dlq_arn,
        "maxReceiveCount": 5,
    })
    record_resp = sqs.create_queue(
        QueueName=SQS_RECORD_QUEUE,
        Attributes={
            "RedrivePolicy": record_redrive,
            "VisibilityTimeout": "30",
        },
    )
    record_queue_url = record_resp["QueueUrl"]
    record_queue_attrs = sqs.get_queue_attributes(QueueUrl=record_queue_url, AttributeNames=["QueueArn"])
    record_queue_arn = record_queue_attrs["Attributes"]["QueueArn"]
    print(f"[SQS] Fila registros criada: {record_queue_url}")

    # --- Subscribe SQS notify to SNS ---
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

    # Set SQS queue policy to allow SNS to send messages
    policy = {
        "Version": "2012-10-17",
        "Id": "SNSPublishPolicy",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "SQS:SendMessage",
                "Resource": notif_queue_arn,
                "Condition": {
                    "ArnEquals": {
                        "aws:SourceArn": topic_arn,
                    }
                },
            }
        ],
    }
    sqs.set_queue_attributes(
        QueueUrl=notif_queue_url,
        Attributes={"Policy": json.dumps(policy)},
    )
    print("[SQS] Politica de acesso configurada na fila de notificacao")

    # --- Summary ---
    print("\n" + "=" * 60)
    print("INFRAESTRUTURA CONFIGURADA")
    print("=" * 60)
    print(f"  S3 Bucket:             {S3_BUCKET}")
    print(f"  SNS Topic ARN:         {topic_arn}")
    print(f"  SNS Topic Name:        {SNS_TOPIC_NAME}")
    print(f"  Notif DLQ URL:         {notif_dlq_url}")
    print(f"  Notif DLQ ARN:         {notif_dlq_arn}")
    print(f"  Notif Queue URL:       {notif_queue_url}")
    print(f"  Notif Queue ARN:       {notif_queue_arn}")
    print(f"  Record DLQ URL:        {record_dlq_url}")
    print(f"  Record DLQ ARN:        {record_dlq_arn}")
    print(f"  Record Queue URL:      {record_queue_url}")
    print(f"  Record Queue ARN:      {record_queue_arn}")
    print(f"  Subscription ARN:      {sub_arn}")


if __name__ == "__main__":
    main()
