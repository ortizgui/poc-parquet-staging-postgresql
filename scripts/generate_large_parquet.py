#!/usr/bin/env python3
"""Gera arquivos Parquet com row groups controlados, para o teste de paginacao/memoria.

O ponto central: no Parquet, **o row group e a unidade de I/O e o piso de memoria
do leitor** — nao o tamanho do arquivo. Este script permite variar
`--row-group-rows` para provar exatamente isso contra o limite de memoria do pod.

Uso:
    # ~1GB, schema largo, row groups de 20k linhas
    python3 scripts/generate_large_parquet.py --target-size-mb 1024 \
        --row-group-rows 20000 --columns 40 --output data/large_1gb.parquet

    # cenario de producao: 200k linhas, schema largo, 10 row groups
    python3 scripts/generate_large_parquet.py --rows 200000 \
        --row-group-rows 20000 --columns 40 --output data/prod_200k.parquet

    # arquivo pequeno, so pra ver o loop de paginacao no log do worker
    python3 scripts/generate_large_parquet.py --rows 10000 \
        --row-group-rows 1000 --output data/demo_paginacao.parquet

    # gerar e subir pro S3 (dispara o fluxo via S3 Event Notification)
    python3 scripts/generate_large_parquet.py --target-size-mb 1024 \
        --row-group-rows 20000 --columns 40 --upload

Ao final imprime o resumo do arquivo (rows, row groups, tamanho descomprimido
por row group) e a estimativa de pico de memoria do leitor .NET.
"""

import argparse
import os
import sys
import time
from datetime import date, timedelta

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
except ImportError:
    pass

S3_BUCKET = os.getenv("S3_BUCKET", "poc-bucket")
S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "test")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

ACCOUNT_COUNT = 10_000
ASSETS = ["PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3", "PERM4", "RENT3", "RADL3",
          "HAPV3", "WEGE3", "CCRO3", "EMBR3", "GGBR4", "CSNA3", "USIM5", "GOAU4",
          "BRAP4", "VALE5", "FIBR3", "CPFE3", "MGLU3", "BBAS3", "SANB11", "ELET3"]

WORDS = ["alpha", "brazil", "custodia", "liquidacao", "mercado", "balcao", "fundo",
         "investidor", "posicao", "ativo", "carteira", "corretora", "operacao",
         "renda", "variavel", "pregao", "settlement", "clearing", "custody",
         "notional", "counterparty", "settlement", "blockchain", "ledger",
         "reference", "institutional", "retail", "derivative", "equity", "bond"]

ALNUM = np.frombuffer(b"abcdefghijklmnopqrstuvwxyz0123456789", dtype=np.uint8)


def _random_alnum(rng, n, width):
    """Coluna de texto aleatorio (alta cardinalidade, comprime mal)."""
    idx = rng.integers(0, len(ALNUM), size=(n, width))
    raw = ALNUM[idx].tobytes()
    arr = pa.array(np.frombuffer(raw, dtype=f"S{width}"))
    return arr.cast(pa.string())


def _word_text(rng, n, n_words=6):
    """Coluna de texto a partir de palavras (cardinalidade media, comprime bem)."""
    words = np.array(WORDS, dtype=object)
    parts = [pa.array(words[rng.integers(0, len(words), size=n)]) for _ in range(n_words)]
    return pc.binary_join_element_wise(*parts, pa.scalar(" "))


def extra_column_names(n_columns):
    """Nomes das colunas extras (as 5 primeiras sao o schema da POC)."""
    return [f"col_{i:02d}" for i in range(6, n_columns + 1)]


def build_chunk(rng, n_rows, n_columns, base_date):
    """Monta um chunk (== um row group) em memoria. Retorna pyarrow.Table."""
    cols = {
        "account_id": pa.array([f"ACC{i:05d}" for i in rng.integers(1, ACCOUNT_COUNT + 1, size=n_rows)]),
        "asset_id": pa.array(np.array(ASSETS, dtype=object)[rng.integers(0, len(ASSETS), size=n_rows)]),
        "reference_date": pa.array(
            [base_date - timedelta(days=int(d)) for d in rng.integers(0, 31, size=n_rows)],
            type=pa.date32(),
        ),
        "quantity": pa.array(np.round(rng.uniform(10, 10_000, size=n_rows), 4)),
        "amount": pa.array(np.round(rng.uniform(100, 1_000_000, size=n_rows), 2)),
    }
    for i, name in enumerate(extra_column_names(n_columns)):
        # alterna: texto de palavras (comprime bem) e aleatorio (comprime mal)
        cols[name] = _word_text(rng, n_rows, 6) if i % 2 == 0 else _random_alnum(rng, n_rows, 32)
    return pa.table(cols)


def human(size_bytes):
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def report(path, container_limit_mb=None):
    """Le os metadados do arquivo e imprime o resumo do teste."""
    pf = pq.ParquetFile(path)
    md = pf.metadata
    sizes = [md.row_group(i).total_byte_size for i in range(md.num_row_groups)]
    disk = os.path.getsize(path)

    print()
    print("=" * 72)
    print("RESUMO DO ARQUIVO")
    print("=" * 72)
    print(f"  arquivo            : {path}")
    print(f"  tamanho em disco   : {human(disk)}")
    print(f"  linhas             : {md.num_rows:,}")
    print(f"  colunas            : {md.num_columns}")
    print(f"  row groups         : {md.num_row_groups}")
    print(f"  linhas / row group : {md.row_group(0).num_rows:,} (primeiro)")
    print(f"  row group (descomp): min {human(min(sizes))} | avg {human(sum(sizes)/len(sizes))} | max {human(max(sizes))}")
    print(f"  compressao         : {disk/max(sum(sizes),1)*100:.1f}% do descomprimido")
    print()
    print(f"  MAIOR ROW GROUP DESCOMPRIMIDO: {human(max(sizes))}  <-- e este o piso de memoria do leitor")
    print(f"  estimativa .NET (3x)         : {human(max(sizes)*3)}")
    if container_limit_mb:
        est = max(sizes) * 3 / 1024 / 1024
        verdict = "OK" if est < container_limit_mb * 0.6 else "RISCO DE OOM"
        print(f"  limite do pod                : {container_limit_mb} MB  ->  {verdict} ({est:.0f} MB estimados)")
    print("=" * 72)
    return {
        "rows": md.num_rows,
        "row_groups": md.num_row_groups,
        "max_row_group_bytes": max(sizes),
        "disk_bytes": disk,
    }


def upload(path, bucket, key):
    import boto3
    from botocore.config import Config
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )
    print(f"\n[S3] upload {path} -> s3://{bucket}/{key}")
    s3.upload_file(path, bucket, key)
    print("[S3] ok")


def main():
    p = argparse.ArgumentParser(description="Gera Parquet grande com row groups controlados")
    p.add_argument("--target-size-mb", type=int, default=None, help="tamanho alvo do arquivo, em MB")
    p.add_argument("--rows", type=int, default=None, help="numero exato de linhas (ignora --target-size-mb)")
    p.add_argument("--row-group-rows", type=int, default=20_000, help="linhas por row group (default: 20000)")
    p.add_argument("--columns", type=int, default=5, help="total de colunas (5 = schema da POC; >5 = schema largo)")
    p.add_argument("--output", default="data/large.parquet", help="caminho de saida")
    p.add_argument("--limit-mb", type=int, default=None, help="limite de memoria do pod, so pra estimativa")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--upload", action="store_true", help="sobe pro S3 da POC (LocalStack)")
    p.add_argument("--key", default=None, help="chave S3 (default: input/<nome do arquivo>)")
    args = p.parse_args()

    if args.columns < 5:
        sys.exit("erro: --columns minimo e 5 (schema da POC)")
    if args.rows is None and args.target_size_mb is None:
        args.target_size_mb = 1024

    os.makedirs(os.path.dirname(os.path.abspath(args.output)) or ".", exist_ok=True)

    rng = np.random.default_rng(args.seed)
    base_date = date.today()
    target_bytes = (args.target_size_mb or 0) * 1024 * 1024

    print(f"[gen] row_group_rows={args.row_group_rows:,} columns={args.columns} "
          f"alvo={'%.0f MB' % args.target_size_mb if args.target_size_mb else 'n/d'} "
          f"rows={args.rows or 'n/d'}")

    writer = pq.ParquetWriter(args.output, build_chunk(rng, 1, args.columns, base_date).schema,
                              compression="snappy")
    rows_written = 0
    row_groups = 0
    last_report = time.time()
    started = time.time()

    try:
        while True:
            if args.rows is not None and rows_written >= args.rows:
                break
            if args.rows is None and rows_written > 0:
                # checa o tamanho a cada ~5% do alvo para nao estourar muito
                if os.path.getsize(args.output) >= target_bytes:
                    break

            n = args.row_group_rows
            if args.rows is not None:
                n = min(n, args.rows - rows_written)

            chunk = build_chunk(rng, n, args.columns, base_date)
            # cada write_table com exatamente row_group_rows linhas vira 1 row group
            writer.write_table(chunk, row_group_size=args.row_group_rows)
            rows_written += n
            row_groups += 1
            del chunk

            if time.time() - last_report > 5:
                size = os.path.getsize(args.output)
                pct = f"{size/target_bytes*100:.1f}%" if target_bytes else f"{rows_written:,} linhas"
                print(f"[gen] rows={rows_written:,} row_groups={row_groups} size={human(size)} ({pct})")
                last_report = time.time()
    finally:
        writer.close()

    info = report(args.output, args.limit_mb)
    print(f"[gen] concluido em {time.time()-started:.1f}s")

    if args.upload:
        key = args.key or f"input/{os.path.basename(args.output)}"
        upload(args.output, S3_BUCKET, key)

    return 0


if __name__ == "__main__":
    sys.exit(main())
