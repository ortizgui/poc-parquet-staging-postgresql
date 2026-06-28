"""Merge records from staging table to principal table.

Este script é destinado a rodar como CRON/JobScheduler ou em modo continuo.
Ele processa registros da tabela staging em batches controlados.

Fluxo:
  1. Seleciona batch de registros da staging
  2. INSERT novos registros na principal (ON CONFLICT DO NOTHING)
  3. UPDATE registros existentes (apenas se mudou)
  4. DELETE da staging após sucesso do upsert
  5. Repete até staging vazia (ou indefinidamente em modo --continuous)

Características:
  - Throttling configurável (delay entre batches)
  - Batch size configurável
  - Lock via pg_advisory_lock para evitar execuções concorrentes
  - Modo continuo para testes end-to-end
  - Métricas de tempo e throughput

Uso:
  python3 scripts/merge_staging.py                    # Modo único (original)
  python3 scripts/merge_staging.py --continuous        # Modo continuo (para testes)
  python3 scripts/merge_staging.py --continuous --max-iterations 1000
"""

import argparse
import csv
import os
import signal
import sys
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

# Global for signal handling
shutdown_requested = False


def signal_handler(signum, frame):
    global shutdown_requested
    print("\n[MERGE] Shutdown requested, finishing current batch...")
    shutdown_requested = True


def merge_batch(conn, cur, batch_ids):
    """Executa merge de um batch específico.
    
    Usa INSERT ... ON CONFLICT para evitar erros de unique constraint.
    O ON CONFLICT DO NOTHING simplesmente ignora registros que ja existem.
    """
    # 1. INSERT com ON CONFLICT DO NOTHING (ignora duplicatas)
    # Depois faz UPDATE apenas para os registros que ja existiam
    cur.execute("""
        INSERT INTO custody_position (account_id, asset_id, reference_date, quantity, amount, created_at)
        SELECT s.account_id, s.asset_id, s.reference_date, s.quantity, s.amount, s.created_at
        FROM custody_position_staging s
        WHERE s.id = ANY(%s)
        ON CONFLICT (account_id, asset_id, reference_date) DO NOTHING
        RETURNING account_id, asset_id, reference_date
    """, (batch_ids,))
    
    # get nb of inserted rows - we need to count manually since RETURNING only gives inserted
    # Actually, let's count total rows we tried to insert and subtract what's in staging after
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


def run_merge_cycle(conn, cur, batch_size, delay_seconds, stats, csv_writer=None, csv_file=None):
    """Executa um ciclo de merge (busca e processa um batch)."""
    # Seleciona próximo batch
    cur.execute("""
        SELECT id
        FROM custody_position_staging
        ORDER BY id
        LIMIT %s
    """, (batch_size,))
    
    batch_rows = cur.fetchall()
    if not batch_rows:
        return False  # Nenhum registro para processar

    batch_ids = [row[0] for row in batch_rows]
    stats['batch_num'] += 1
    
    batch_start = time.time()
    
    try:
        # Executa merge do batch
        inserted, updated, deleted = merge_batch(conn, cur, batch_ids)
        conn.commit()
        batch_time = time.time() - batch_start
        
        stats['total_inserted'] += inserted
        stats['total_updated'] += updated
        stats['total_deleted'] += deleted

        # Métricas
        elapsed = time.time() - stats['start_time']
        throughput = (stats['total_inserted'] + stats['total_updated']) / elapsed if elapsed > 0 else 0
        
        print(f"  [BATCH {stats['batch_num']}] +{inserted}ins ~{updated}upd -{deleted}del | "
              f"{batch_time*1000:.0f}ms | {throughput:.0f} regs/s")

        if csv_writer:
            csv_writer.writerow({
                'timestamp': elapsed,
                'batch': stats['batch_num'],
                'batch_time_ms': batch_time * 1000,
                'inserted': inserted,
                'updated': updated,
                'total_processed': stats['total_inserted'] + stats['total_updated'],
                'pending_locks': 0,
                'dead_custody': 0,
                'dead_staging': 0,
                'cache_hit_ratio': 0,
                'long_transactions': 0
            })
            csv_file.flush()

        # Throttle entre batches
        if delay_seconds > 0:
            time.sleep(delay_seconds)
        
        return True

    except Exception as e:
        conn.rollback()
        print(f"  [BATCH {stats['batch_num']}] ERROR: {e}")
        # Continue to next batch after a small delay
        time.sleep(delay_seconds)
        return True


def main():
    global shutdown_requested
    
    parser = argparse.ArgumentParser(description="Merge staging to principal table")
    parser.add_argument("--continuous", action="store_true",
                        help="Run continuously until shutdown (for end-to-end tests)")
    parser.add_argument("--max-iterations", type=int, default=0,
                        help="Max iterations in continuous mode (0=unlimited)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"Batch size (default: {BATCH_SIZE})")
    parser.add_argument("--delay", type=float, default=MERGE_DELAY_SECONDS,
                        help=f"Delay between batches in seconds (default: {MERGE_DELAY_SECONDS})")
    parser.add_argument("--metrics-csv", type=str, default="",
                        help="Output CSV file for batch metrics")
    args = parser.parse_args()
    
    batch_size = args.batch_size
    delay_seconds = args.delay
    continuous = args.continuous
    max_iterations = args.max_iterations

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    # Set connection to autocommit=False (default) but handle explicitly
    cur = conn.cursor()

    # Setup signal handlers for continuous mode
    if continuous:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Adquire lock para evitar execuções concorrentes
        cur.execute("SELECT pg_try_advisory_lock(%s)", (MERGE_LOCK_ID,))
        acquired = cur.fetchone()[0]
        
        if not acquired:
            print("[MERGE] Another merge is running. Exiting.")
            return

        print(f"[MERGE] Lock acquired.")
        csv_file = None
        csv_writer = None
        if args.metrics_csv:
            csv_file = open(args.metrics_csv, 'w', newline='')
            csv_writer = csv.DictWriter(csv_file, fieldnames=[
                'timestamp', 'batch', 'batch_time_ms', 'inserted', 'updated',
                'total_processed', 'pending_locks', 'dead_custody', 'dead_staging',
                'cache_hit_ratio', 'long_transactions'
            ])
            csv_writer.writeheader()
        print(f"[MERGE] Config: batch_size={batch_size}, delay={delay_seconds}s")
        if continuous:
            print(f"[MERGE] Mode: CONTINUOUS (Ctrl+C to stop)")
        else:
            print(f"[MERGE] Mode: SINGLE RUN")

        # Conta registros pendentes
        cur.execute("SELECT COUNT(*) FROM custody_position_staging")
        pending = cur.fetchone()[0]
        print(f"[MERGE] Registros na staging: {pending}")

        if pending == 0 and not continuous:
            print("[MERGE] Staging vazia. Nada a processar.")
            return

        stats = {
            'total_inserted': 0,
            'total_updated': 0,
            'total_deleted': 0,
            'batch_num': 0,
            'start_time': time.time(),
        }

        iteration = 0
        while True:
            iteration += 1
            
            # Check max iterations
            if max_iterations > 0 and iteration > max_iterations:
                print(f"[MERGE] Max iterations ({max_iterations}) reached. Stopping.")
                break
            
            # Check shutdown
            if shutdown_requested:
                print(f"[MERGE] Shutdown requested. Stopping after {stats['batch_num']} batches.")
                break
            
            # Executa um ciclo de merge
            had_work = run_merge_cycle(conn, cur, batch_size, delay_seconds, stats, csv_writer, csv_file)
            
            if not had_work:
                if continuous:
                    # No work available, wait and retry
                    print(f"[MERGE] Staging empty, waiting {delay_seconds}s for more data...")
                    time.sleep(delay_seconds)
                    # Re-check pending count
                    cur.execute("SELECT COUNT(*) FROM custody_position_staging")
                    pending = cur.fetchone()[0]
                    if pending == 0:
                        continue  # Keep waiting
                else:
                    break  # Single mode - we're done

        # Tempo total
        total_time = time.time() - stats['start_time']
        overall_throughput = (stats['total_inserted'] + stats['total_updated']) / total_time if total_time > 0 else 0

        print(f"\n[MERGE] {'Continuous mode' if continuous else 'Merge'} concluded!")
        print(f"  Batches processados: {stats['batch_num']}")
        print(f"  Tempo total: {total_time:.2f}s")
        print(f"  Throughput: {overall_throughput:.0f} regs/s")
        print(f"  Inserted: {stats['total_inserted']}")
        print(f"  Updated: {stats['total_updated']}")
        print(f"  Deleted from staging: {stats['total_deleted']}")

    finally:
        # Libera lock
        try:
            cur.execute("SELECT pg_advisory_unlock(%s)", (MERGE_LOCK_ID,))
            print("[MERGE] Lock released.")
        except:
            print("[MERGE] Lock release failed (already released or error)")
        if csv_file:
            csv_file.close()
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
