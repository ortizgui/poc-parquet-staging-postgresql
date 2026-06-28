"""Merge records from staging table to principal table.

Este script é destinado a rodar como CRON/JobScheduler.
Ele processa registros da tabela staging em batches controlados.

Fluxo:
  1. Seleciona batch de registros da staging
  2. INSERT novos registros na principal (ON CONFLICT DO NOTHING)
  3. UPDATE registros existentes (apenas se mudou)
  4. DELETE da staging após sucesso do upsert
  5. Repete até staging vazia

Características:
  - Throttling configurável (delay entre batches)
  - Batch size configurável
  - Lock via pg_advisory_lock para evitar execuções concorrentes
  - Métricas de tempo e throughput

Uso:
  python3 scripts/merge_staging.py
  python3 scripts/merge_staging.py --batch-size 2000 --delay 0.5
"""

import os
import time
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "pocdb")
PG_USER = os.getenv("POSTGRES_USER", "pocuser")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pocpass")

# Configurações de merge
BATCH_SIZE = int(os.getenv("MERGE_BATCH_SIZE", "2000"))
MERGE_DELAY_SECONDS = float(os.getenv("MERGE_DELAY_SECONDS", "0.5"))
MERGE_LOCK_ID = 42


def merge_batch(cur, batch_ids):
    """Executa merge de um batch específico."""
    # 1. INSERT novos registros (que não existem na principal)
    cur.execute("""
        INSERT INTO custody_position (account_id, asset_id, reference_date, quantity, amount, created_at)
        SELECT s.account_id, s.asset_id, s.reference_date, s.quantity, s.amount, s.created_at
        FROM custody_position_staging s
        WHERE s.id = ANY(%s)
          AND NOT EXISTS (
              SELECT 1 FROM custody_position f
              WHERE f.account_id = s.account_id
                AND f.asset_id = s.asset_id
                AND f.reference_date = s.reference_date
          )
    """, (batch_ids,))
    inserted = cur.rowcount

    # 2. UPDATE registros existentes (apenas se valores mudaram)
    cur.execute("""
        UPDATE custody_position f
        SET quantity = s.quantity,
            amount = s.amount,
            updated_at = NOW()
        FROM custody_position_staging s
        WHERE s.id = ANY(%s)
          AND f.account_id = s.account_id
          AND f.asset_id = s.asset_id
          AND f.reference_date = s.reference_date
          AND (f.quantity IS DISTINCT FROM s.quantity
            OR f.amount IS DISTINCT FROM s.amount)
    """, (batch_ids,))
    updated = cur.rowcount

    # 3. DELETE da staging (após upsert bem sucedido)
    cur.execute("""
        DELETE FROM custody_position_staging
        WHERE id = ANY(%s)
    """, (batch_ids,))
    deleted = cur.rowcount

    return inserted, updated, deleted


def main():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    cur = conn.cursor()

    try:
        # Adquire lock para evitar execuções concorrentes
        cur.execute("SELECT pg_try_advisory_lock(%s)", (MERGE_LOCK_ID,))
        acquired = cur.fetchone()[0]
        
        if not acquired:
            print("[MERGE] Another merge is running. Exiting.")
            return

        print(f"[MERGE] Lock acquired. Starting merge process.")
        print(f"[MERGE] Config: batch_size={BATCH_SIZE}, delay={MERGE_DELAY_SECONDS}s")

        # Conta registros pendentes
        cur.execute("SELECT COUNT(*) FROM custody_position_staging")
        pending = cur.fetchone()[0]
        print(f"[MERGE] Registros na staging: {pending}")

        if pending == 0:
            print("[MERGE] Staging vazia. Nada a processar.")
            return

        total_inserted = 0
        total_updated = 0
        total_deleted = 0
        start_time = time.time()
        batch_num = 0

        while True:
            # Seleciona próximo batch
            cur.execute("""
                SELECT id
                FROM custody_position_staging
                ORDER BY id
                LIMIT %s
            """, (BATCH_SIZE,))
            
            batch_rows = cur.fetchall()
            if not batch_rows:
                break

            batch_ids = [row[0] for row in batch_rows]
            batch_num += 1
            
            batch_start = time.time()
            
            # Executa merge do batch
            inserted, updated, deleted = merge_batch(cur, batch_ids)
            
            conn.commit()
            batch_time = time.time() - batch_start
            
            total_inserted += inserted
            total_updated += updated
            total_deleted += deleted

            # Métricas
            elapsed = time.time() - start_time
            throughput = (total_inserted + total_updated) / elapsed if elapsed > 0 else 0
            progress = (total_deleted / pending * 100) if pending > 0 else 100
            
            print(f"  [BATCH {batch_num}] +{inserted}ins ~{updated}upd | "
                  f"{batch_time*1000:.0f}ms | {progress:.1f}% | "
                  f"{throughput:.0f} regs/s")

            # Throttle entre batches
            if MERGE_DELAY_SECONDS > 0:
                time.sleep(MERGE_DELAY_SECONDS)

        # Tempo total
        total_time = time.time() - start_time
        overall_throughput = (total_inserted + total_updated) / total_time if total_time > 0 else 0

        print(f"\n[MERGE] Concluído!")
        print(f"  Tempo total: {total_time:.2f}s")
        print(f"  Throughput: {overall_throughput:.0f} regs/s")
        print(f"  Inserted: {total_inserted}")
        print(f"  Updated: {total_updated}")
        print(f"  Deleted from staging: {total_deleted}")

    finally:
        # Libera lock
        cur.execute("SELECT pg_advisory_unlock(%s)", (MERGE_LOCK_ID,))
        print("[MERGE] Lock released.")
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
