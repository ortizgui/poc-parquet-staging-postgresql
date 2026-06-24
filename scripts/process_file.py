"""
Download and process a Parquet file from S3 using streaming row groups.

Key concepts demonstrated for production scale:
  1. Streaming row groups via s3fs (Range GET requests) — never loads the full file
  2. Parallel row group processing — each worker gets its own S3 + DB connection
  3. Checkpoint/resume — ON CONFLICT + last row tracking
  4. Idempotent error table with UNIQUE constraint
"""

import argparse
import hashlib
import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import pyarrow.parquet as pq
import s3fs
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "pocdb")
PG_USER = os.getenv("POSTGRES_USER", "pocuser")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pocpass")
MAX_WORKERS = int(os.getenv("PROCESS_MAX_WORKERS", "4"))


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def get_db_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


def compute_record_hash(row) -> str:
    raw = (
        f"{row['account_id']}|{row['asset_id']}|{row['reference_date']}|"
        f"{row['quantity']}|{row['amount']}"
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def validate_row(row):
    errors: list[str] = []
    acc_id = str(row.get("account_id", "")).strip()
    asset_id = str(row.get("asset_id", "")).strip()
    ref_date = row.get("reference_date")
    qty = row.get("quantity")
    amt = row.get("amount")

    if not acc_id:
        errors.append("account_id is empty")
    if not asset_id:
        errors.append("asset_id is empty")
    if ref_date is None or pd.isna(ref_date):
        errors.append("reference_date is null")
    if qty is None or pd.isna(qty) or qty < 0:
        errors.append(f"quantity is invalid: {qty}")
    if amt is None or pd.isna(amt) or amt < 0:
        errors.append(f"amount is invalid: {amt}")
    return errors


# ---------------------------------------------------------------------------
# Worker: processes exactly one row-group
# ---------------------------------------------------------------------------

def process_row_group(
    s3_path: str,
    rg_idx: int,
    batch_id: uuid.UUID,
    source_file: str,
    start_row: int,
    chunk_size: int,
) -> dict:
    """Download and process a single Parquet row group.

    Each invocation runs in its own thread and opens independent
    connections to S3 (Range GET streaming) and PostgreSQL.

    Trickle-down memory model.
    ──────────────────────────
    Memoria por worker ≈ tamanho do row group (DataFrame) +
                         listas de tuplas para batch INSERT.

    Para 100k linhas por row group:
      DataFrame: ~30-50 MB
      Listas:    ~10-20 MB
      Total:     ~50-70 MB / worker

    Com 4 workers simultaneos: ~200-280 MB total.

    Controle de memoria via configuracao do row group size
    na geracao do Parquet (ex: row_group_size=50000).
    """
    fs = s3fs.S3FileSystem(
        key="test",
        secret="test",
        client_kwargs={
            "endpoint_url": S3_ENDPOINT,
            "region_name": "us-east-1",
        },
    )
    conn = get_db_conn()
    cur = conn.cursor()

    result = {"rg": rg_idx, "valid": 0, "invalid": 0, "duplicate": 0, "rows": 0}

    try:
        # --- 1. Stream one row-group from S3 via Range requests ---
        #     So o row-group designado e baixado — nunca o arquivo todo.
        with fs.open(s3_path, "rb") as f:
            pf = pq.ParquetFile(f)
            table = pf.read_row_group(rg_idx)

        df = table.to_pandas()
        result["rows"] = len(df)

        # --- 2. Validation loop + build batch lists ---
        #     Percorre as N linhas UMA vez (O(N), inevitavel).
        #     Acumula tuplas para INSERT em batch ao inves de
        #     N inserts individuais.
        valid_rows: list[tuple] = []
        invalid_rows: list[tuple] = []

        for local_idx, (_, row) in enumerate(df.iterrows()):
            row_number = start_row + local_idx
            errors = validate_row(row)
            ref_date = (
                row["reference_date"].to_pydatetime()
                if hasattr(row["reference_date"], "to_pydatetime")
                else row["reference_date"]
            )

            if errors:
                invalid_rows.append((
                    str(batch_id), source_file, row_number,
                    json.dumps({
                        "account_id": str(row.get("account_id", "")),
                        "asset_id": str(row.get("asset_id", "")),
                        "reference_date": str(row.get("reference_date", "")),
                        "quantity": row.get("quantity"),
                        "amount": row.get("amount"),
                    }),
                    "; ".join(errors),
                ))
            else:
                record_hash = compute_record_hash(row)
                valid_rows.append((
                    str(batch_id), source_file, row_number, record_hash,
                    row["account_id"], row["asset_id"],
                    ref_date, row["quantity"], row["amount"],
                ))

        # --- 3. Batch INSERT valid rows (1 statement, N rows) ---
        if valid_rows:
            execute_values(cur, """
                INSERT INTO custody_position_staging
                    (batch_id, source_file, row_number, record_hash,
                     account_id, asset_id, reference_date, quantity, amount)
                VALUES %s
                ON CONFLICT (source_file, row_number) DO NOTHING
            """, valid_rows)
            result["valid"] = cur.rowcount
            result["duplicate"] = len(valid_rows) - cur.rowcount

        # --- 4. Batch INSERT invalid rows (1 statement, N rows) ---
        if invalid_rows:
            execute_values(cur, """
                INSERT INTO custody_position_error
                    (batch_id, source_file, row_number, payload, error_reason)
                VALUES %s
                ON CONFLICT (source_file, row_number) DO NOTHING
            """, invalid_rows)
            result["invalid"] = len(invalid_rows)

        # Libera as listas antes do commit (menos memória em pico)
        del valid_rows
        del invalid_rows
        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Process a Parquet file from S3 into staging (streaming + parallel)"
    )
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument(
        "--chunk-size", type=int, default=5,
        help="Rows per validation batch (delegated to row-group level)",
    )
    args = parser.parse_args()

    batch_id = uuid.uuid4()
    source_file = f"s3://{args.bucket}/{args.key}"
    s3_path = f"s3://{args.bucket}/{args.key}"

    print(f"batch_id: {batch_id}")
    print(f"Arquivo: {source_file}")
    print(f"Workers: {MAX_WORKERS}")

    # -----------------------------------------------------------------------
    # Phase 1 — Stream Parquet metadata (footer only, via Range request)
    # -----------------------------------------------------------------------
    fs = s3fs.S3FileSystem(
        key="test",
        secret="test",
        client_kwargs={
            "endpoint_url": S3_ENDPOINT,
            "region_name": "us-east-1",
        },
    )

    with fs.open(s3_path, "rb") as f:
        pf = pq.ParquetFile(f)
        total_rows = pf.metadata.num_rows
        num_row_groups = pf.metadata.num_row_groups
        print(f"Total linhas: {total_rows} | Row groups: {num_row_groups}")

        # Build row-group → global-row-range mapping
        row_group_ranges: list[tuple[int, int, int]] = []
        current_row = 0
        for rg_idx in range(num_row_groups):
            rg_rows = pf.metadata.row_group(rg_idx).num_rows
            row_group_ranges.append((rg_idx, current_row, current_row + rg_rows))
            current_row += rg_rows

    # -----------------------------------------------------------------------
    # Phase 2 — Checkpoint: skip row groups already fully processed
    # -----------------------------------------------------------------------
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT COALESCE(MAX(row_number), -1) FROM custody_position_staging WHERE source_file = %s",
        (source_file,),
    )
    last_processed = cur.fetchone()[0]
    cur.close()
    conn.close()

    rg_to_process = [
        (rg_idx, start)
        for rg_idx, start, end in row_group_ranges
        if start > last_processed
    ]
    print(f"Row groups pendentes: {len(rg_to_process)}/{num_row_groups}")

    if not rg_to_process:
        print("Nada a processar (checkpoint retomou de onde parou).")
        return

    # -----------------------------------------------------------------------
    # Phase 3 — Parallel row-group processing
    # -----------------------------------------------------------------------
    aggregator = {"valid": 0, "invalid": 0, "duplicate": 0, "rows": 0}

    with ThreadPoolExecutor(
        max_workers=min(MAX_WORKERS, len(rg_to_process))
    ) as executor:
        future_map = {}
        for rg_idx, rg_start in rg_to_process:
            fut = executor.submit(
                process_row_group,
                s3_path,
                rg_idx,
                batch_id,
                source_file,
                rg_start,
                args.chunk_size,
            )
            future_map[fut] = rg_idx

        for future in as_completed(future_map):
            rg_idx = future_map[future]
            try:
                res = future.result()
                for k in ("valid", "invalid", "rows"):
                    aggregator[k] += res[k]
                aggregator["duplicate"] += res.get("duplicate", 0)
                print(
                    f"  RG {rg_idx:>2}: {res['valid']:>3} validos, "
                    f"{res['invalid']:>3} invalidos, "
                    f"{res['duplicate']:>3} duplicatas, "
                    f"{res['rows']:>3} linhas"
                )
            except Exception as e:
                print(f"  [ERRO] Row group {rg_idx}: {e}")

    # -----------------------------------------------------------------------
    # Report
    # -----------------------------------------------------------------------
    print(f"\nResumo final:")
    print(f"  batch_id:          {batch_id}")
    print(f"  total lido:        {aggregator['rows']}")
    print(f"  total valido:      {aggregator['valid']}")
    print(f"  total invalido:    {aggregator['invalid']}")
    print(f"  duplicatas:        {aggregator['duplicate']}")
    print(f"  row groups proc.:  {len(rg_to_process)}")


if __name__ == "__main__":
    main()
