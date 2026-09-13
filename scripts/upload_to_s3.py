"""Upload a local Parquet file to the POC bucket.

Sem argumentos, mantem o comportamento historico (o sample de
`data/input/custody_position.parquet`). Com `--file`/`--key`, sobe qualquer
arquivo — necessario para o A/B de row group, onde os arquivos de teste sao
gerados com nomes e tamanhos diferentes.

Uso:
    python3 scripts/upload_to_s3.py
    python3 scripts/upload_to_s3.py --file data/ab_1160000_1rg.parquet
    python3 scripts/upload_to_s3.py --file x.parquet --key input/outro_nome.parquet
"""

import argparse
import os

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
S3_BUCKET = os.getenv("S3_BUCKET", "poc-bucket")
DEFAULT_FILE = "./data/input/custody_position.parquet"


def main():
    p = argparse.ArgumentParser(description="Sobe um arquivo para o bucket da POC.")
    p.add_argument("--file", default=DEFAULT_FILE, help=f"arquivo local (default: {DEFAULT_FILE})")
    p.add_argument("--key", default=None, help="chave S3 (default: input/<nome do arquivo>)")
    p.add_argument("--bucket", default=S3_BUCKET, help=f"bucket (default: {S3_BUCKET})")
    args = p.parse_args()

    key = args.key or f"input/{os.path.basename(args.file)}"

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )

    try:
        s3.create_bucket(Bucket=args.bucket)
        print(f"Bucket criado: {args.bucket}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket ja existe: {args.bucket}")
    except Exception as e:
        print(f"Aviso ao criar bucket: {e}")

    if not os.path.exists(args.file):
        raise SystemExit(f"erro: arquivo nao encontrado: {args.file}")

    s3.upload_file(args.file, args.bucket, key)
    size = os.path.getsize(args.file) / 1024 / 1024
    print(f"Arquivo enviado: s3://{args.bucket}/{key} ({size:.1f} MB)")


if __name__ == "__main__":
    main()
