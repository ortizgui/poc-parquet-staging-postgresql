"""Load simulation tool for the POC with database metrics collection.

Usage:
    python3 scripts/simulate_load.py --existing-records 100000 --ingestion-size 10000 --update-ratio 60
"""

import argparse
import uuid
import random
import time
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "pocdb")
PG_USER = os.getenv("POSTGRES_USER", "pocuser")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pocpass")

MERGE_LOCK_ID = 42
ACCOUNTS = [f"ACC{i:03d}" for i in range(1, 10001)]
ASSETS = ["PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3", "PERM4", "RENT3", "RADL3", "HAPV3", "WEGE3",
          "CCRO3", "EMBR3", "GGBR4", "CSNA3", "USIM5", "GOAU4", "BRAP4", "VALE5", "FIBR3", "CPFE3"]


def generate_unique_records(count, days_back=30):
    """Generate records with unique (account_id, asset_id, reference_date) combos."""
    records = []
    base_date = datetime.now().date()
    dates = [base_date - timedelta(days=i) for i in range(days_back + 1)]
    
    seen = set()
    attempts = 0
    max_attempts = count * 3
    
    while len(records) < count and attempts < max_attempts:
        attempts += 1
        account_id = random.choice(ACCOUNTS)
        asset_id = random.choice(ASSETS)
        reference_date = random.choice(dates)
        
        key = (account_id, asset_id, reference_date)
        if key in seen:
            continue
            
        seen.add(key)
        quantity = round(random.uniform(10, 10000), 4)
        amount = round(random.uniform(100, 1000000), 2)
        records.append((account_id, asset_id, reference_date, quantity, amount))
    
    if len(records) < count:
        print(f"[WARN] Only generated {len(records)} unique records (requested {count})")
    
    return records


def seed_principal_table(cursor, count):
    """Seed principal table, always truncating first for clean state."""
    print(f"[SEED] Truncating and seeding principal table with {count} records...")
    cursor.execute("TRUNCATE TABLE custody_position CASCADE")
    
    records = generate_unique_records(count)
    
    execute_values(
        cursor,
        """INSERT INTO custody_position (account_id, asset_id, reference_date, quantity, amount)
           VALUES %s""",
        records,
        page_size=10000
    )
    cursor.connection.commit()
    print(f"[SEED] Principal table seeded with {count} records")


def clear_buffer_table(cursor):
    cursor.execute("TRUNCATE TABLE custody_position_buffer CASCADE")
    cursor.connection.commit()
    print("[SETUP] Buffer table cleared")


def collect_db_metrics(cursor):
    """Collect database metrics before merge starts."""
    metrics = {}
    
    # Database size
    cursor.execute("""
        SELECT pg_database_size(%s) / 1024 / 1024 as size_mb
    """, (PG_DB,))
    metrics['db_size_mb'] = cursor.fetchone()[0]
    
    # Table sizes
    cursor.execute("""
        SELECT 
            c.relname,
            pg_stat_get_live_tup(c.oid) as live_tuples,
            pg_stat_get_dead_tup(c.oid) as dead_tuples
        FROM pg_class c
        WHERE c.relname IN ('custody_position', 'custody_position_buffer')
        ORDER BY c.relname
    """)
    for row in cursor.fetchall():
        metrics[f'{row[0]}_live'] = row[1]
        metrics[f'{row[0]}_dead'] = row[2]
    
    # Active connections
    cursor.execute("""
        SELECT state, COUNT(*) 
        FROM pg_stat_activity 
        WHERE datname = %s
        GROUP BY state
    """, (PG_DB,))
    metrics['connections'] = dict(cursor.fetchall())
    
    # Lock info
    cursor.execute("""
        SELECT COUNT(*) 
        FROM pg_locks 
        WHERE granted = false
    """)
    metrics['pending_locks'] = cursor.fetchone()[0]
    
    return metrics


def collect_batch_metrics(cursor, batch_num):
    """Collect metrics during merge."""
    metrics = {'batch': batch_num, 'timestamp': datetime.now().strftime('%H:%M:%S')}
    
    # Get memory stats from pg_stat_activity (useful for PostgreSQL 13+)
    try:
        cursor.execute("""
            SELECT COUNT(*) FROM pg_stat_activity 
            WHERE state = 'active' AND query != '<IDLE>'
        """)
        metrics['active_queries'] = cursor.fetchone()[0]
    except:
        metrics['active_queries'] = 'N/A'
    
    # Dead tuples in tables being merged
    cursor.execute("""
        SELECT 
            c.relname,
            pg_stat_get_dead_tup(c.oid) as dead_tuples
        FROM pg_class c
        WHERE c.relname IN ('custody_position', 'custody_position_buffer')
    """)
    for row in cursor.fetchall():
        metrics[f'{row[0]}_dead'] = row[1]
    
    # Long running queries
    cursor.execute("""
        SELECT MAX(EXTRACT(EPOCH FROM (NOW() - state_change))) * 1000 as oldest_query_ms
        FROM pg_stat_activity 
        WHERE state = 'active' AND query != '<IDLE>'
    """)
    metrics['oldest_query_ms'] = cursor.fetchone()[0]
    
    return metrics


def run_merge(cursor, conn, batch_size, delay):
    cursor.execute("SELECT pg_advisory_lock(%s)", (MERGE_LOCK_ID,))
    print("[MERGE] Advisory lock adquirido (lock_id=42)")

    cursor.execute("SELECT COUNT(*) FROM custody_position_buffer WHERE status = 'PENDING'")
    pending_before = cursor.fetchone()[0]

    if pending_before == 0:
        cursor.execute("SELECT pg_advisory_unlock(%s)", (MERGE_LOCK_ID,))
        return 0, [], 0, {}

    total_merged = 0
    batch_results = []
    batch_metrics = []
    start_time = time.time()
    
    # Collect initial metrics
    initial_metrics = collect_db_metrics(cursor)
    print(f"[METRICS] Initial - DB size: {initial_metrics.get('db_size_mb', 'N/A')}MB, "
          f"Connections: {initial_metrics.get('connections', {})}, "
          f"Pending locks: {initial_metrics.get('pending_locks', 'N/A')}")

    while True:
        batch_start = time.time()
        batch_num = total_merged // batch_size + 1
        
        # Collect pre-batch metrics
        pre_batch_metrics = collect_batch_metrics(cursor, batch_num)
        
        cursor.execute("""
            SELECT id
            FROM custody_position_buffer
            WHERE status = 'PENDING'
            ORDER BY id
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        """, (batch_size,))

        batch_rows = cursor.fetchall()
        if not batch_rows:
            break

        batch_ids = [row[0] for row in batch_rows]

        cursor.execute("""
            INSERT INTO custody_position (account_id, asset_id, reference_date, quantity, amount)
            SELECT s.account_id, s.asset_id, s.reference_date, s.quantity, s.amount
            FROM custody_position_buffer s
            WHERE s.id = ANY(%s)
              AND NOT EXISTS (
                  SELECT 1 FROM custody_position f
                  WHERE f.account_id = s.account_id
                    AND f.asset_id = s.asset_id
                    AND f.reference_date = s.reference_date
              )
        """, (batch_ids,))
        inserted = cursor.rowcount

        cursor.execute("""
            UPDATE custody_position f
            SET quantity = s.quantity,
                amount = s.amount,
                updated_at = NOW()
            FROM custody_position_buffer s
            WHERE s.id = ANY(%s)
              AND f.account_id = s.account_id
              AND f.asset_id = s.asset_id
              AND f.reference_date = s.reference_date
              AND (f.quantity IS DISTINCT FROM s.quantity
                OR f.amount IS DISTINCT FROM s.amount)
        """, (batch_ids,))
        updated = cursor.rowcount

        cursor.execute("""
            UPDATE custody_position_buffer
            SET status = 'MERGED', merged_at = NOW()
            WHERE id = ANY(%s)
        """, (batch_ids,))

        conn.commit()
        total_merged += len(batch_ids)
        batch_time = time.time() - batch_start
        
        # Collect post-batch metrics
        post_batch_metrics = collect_batch_metrics(cursor, batch_num)
        batch_metrics.append({
            'batch': batch_num,
            'time': batch_time,
            'inserted': inserted,
            'updated': updated,
            'dead_custody': post_batch_metrics.get('custody_position_dead', 0),
            'dead_buffer': post_batch_metrics.get('custody_position_buffer_dead', 0)
        })
        
        pct = (total_merged / pending_before) * 100
        total_batches = (pending_before + batch_size - 1) // batch_size
        print(f"  [MERGE] batch {batch_num}/{total_batches}: "
              f"+{inserted} ins, ~{updated} upd ({pct:.0f}%) - {batch_time:.3f}s "
              f"| dead_tuples: cp={post_batch_metrics.get('custody_position_dead', 0)}, "
              f"buf={post_batch_metrics.get('custody_position_buffer_dead', 0)}")

        if delay > 0:
            time.sleep(delay)

    cursor.execute("SELECT pg_advisory_unlock(%s)", (MERGE_LOCK_ID,))
    print("[MERGE] Advisory lock liberado")

    cursor.execute("DELETE FROM custody_position_buffer WHERE status = 'MERGED'")
    cursor.connection.commit()

    total_time = time.time() - start_time
    
    # Collect final metrics
    final_metrics = collect_db_metrics(cursor)
    
    return total_merged, batch_results, total_time, {
        'initial': initial_metrics,
        'final': final_metrics,
        'batches': batch_metrics
    }


def main():
    parser = argparse.ArgumentParser(description="Load simulation tool")
    parser.add_argument("--existing-records", type=int, default=100000,
                        help="Records already in principal table")
    parser.add_argument("--ingestion-size", type=int, default=10000,
                        help="Records to ingest")
    parser.add_argument("--update-ratio", type=int, default=60,
                        help="Percentage of records that update existing combos")
    parser.add_argument("--batch-size", type=int, default=2000,
                        help="Merge batch size")
    parser.add_argument("--delay", type=float, default=0.5,
                        help="Delay between batches (seconds)")
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    cursor = conn.cursor()

    # Always seed fresh to ensure clean state
    seed_principal_table(cursor, args.existing_records)
    clear_buffer_table(cursor)

    update_count = int(args.ingestion_size * args.update_ratio / 100)
    insert_count = args.ingestion_size - update_count

    print(f"[GEN] Generating {args.ingestion_size} records ({args.update_ratio}% updates = {update_count}, {100-args.update_ratio}% inserts = {insert_count})...")

    # Get existing combos for updates
    cursor.execute("""
        SELECT account_id, asset_id, reference_date
        FROM custody_position
        ORDER BY random()
        LIMIT %s
    """, (update_count,))
    existing_combos = set((row[0], row[1], row[2]) for row in cursor.fetchall())
    
    print(f"[GEN] Found {len(existing_combos)} existing combos for updates")

    # Get all existing combos to avoid for inserts
    cursor.execute("SELECT account_id, asset_id, reference_date FROM custody_position")
    all_existing_combos = set((row[0], row[1], row[2]) for row in cursor.fetchall())
    
    # Also track what we're going to insert to avoid internal duplicates
    planned_insert_combos = set()
    
    base_date = datetime.now().date()
    dates = [base_date - timedelta(days=i) for i in range(31)]
    
    # Generate update records (using existing combos - these will definitely update)
    source_file = f"sim_{uuid.uuid4().hex[:8]}.parquet"
    batch_uuid = str(uuid.uuid4())
    update_records = []
    
    for i, combo in enumerate(existing_combos):
        account_id, asset_id, reference_date = combo
        quantity = round(random.uniform(10, 10000), 4)
        amount = round(random.uniform(100, 1000000), 2)
        record_hash = uuid.uuid4().hex[:16]
        update_records.append((batch_uuid, source_file, i + 1, record_hash,
                            account_id, asset_id, reference_date, quantity, amount, 'PENDING'))

    # Generate insert records (NEW combos that don't exist in principal table)
    insert_records = []
    attempts = 0
    max_attempts = insert_count * 10
    next_row_number = len(update_records) + 1
    
    while len(insert_records) < insert_count and attempts < max_attempts:
        attempts += 1
        account_id = random.choice(ACCOUNTS)
        asset_id = random.choice(ASSETS)
        reference_date = random.choice(dates)
        
        key = (account_id, asset_id, reference_date)
        
        # Must not exist in principal table AND not be a duplicate within our inserts
        if key in all_existing_combos or key in planned_insert_combos:
            continue
        
        planned_insert_combos.add(key)
        quantity = round(random.uniform(10, 10000), 4)
        amount = round(random.uniform(100, 1000000), 2)
        record_hash = uuid.uuid4().hex[:16]
        insert_records.append((batch_uuid, source_file, next_row_number, record_hash,
                            account_id, asset_id, reference_date, quantity, amount, 'PENDING'))
        next_row_number += 1

    if len(insert_records) < insert_count:
        print(f"[WARN] Only generated {len(insert_records)} unique insert records (needed {insert_count})")

    # Combine and shuffle
    all_records = update_records + insert_records
    random.shuffle(all_records)

    print(f"[GEN] Prepared {len(update_records)} update + {len(insert_records)} insert = {len(all_records)} total records")
    print(f"[GEN] Inserting into buffer table...")

    execute_values(
        cursor,
        """INSERT INTO custody_position_buffer
           (batch_id, source_file, row_number, record_hash, account_id, asset_id,
            reference_date, quantity, amount, status)
           VALUES %s""",
        all_records,
        page_size=5000
    )
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM custody_position_buffer WHERE status = 'PENDING'")
    buffer_count = cursor.fetchone()[0]
    print(f"[GEN] Buffer table has {buffer_count} PENDING records")

    print(f"\n[MERGE] Starting merge with batch_size={args.batch_size}, delay={args.delay}s...")
    total_merged, batch_results, total_time, metrics = run_merge(cursor, conn, args.batch_size, args.delay)

    throughput = total_merged / total_time if total_time > 0 else 0

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    update_pct = args.update_ratio
    insert_pct = 100 - args.update_ratio

    print("\n" + "=" * 70)
    print("=== LOAD SIMULATION REPORT ===")
    print(f"Date: {timestamp}")
    print(f"Existing Records: {args.existing_records:,}")
    print(f"Ingestion Size: {args.ingestion_size:,} ({update_pct}% updates, {insert_pct}% inserts)")
    print(f"Batch Size: {args.batch_size}")
    print(f"Delay: {args.delay}s")
    print()
    print("=== RESULTS ===")
    print(f"Total Time: {total_time:.3f}s")
    print(f"Throughput: {throughput:.1f} records/second")
    print(f"Batches: {len(metrics['batches'])}")
    print("Per-batch breakdown:")
    for b in metrics['batches']:
        print(f"  - Batch {b['batch']}: {b['inserted']} ins, {b['updated']} upd - {b['time']:.3f}s (dead: cp={b['dead_custody']}, buf={b['dead_buffer']})")
    print()
    print("=== DATABASE METRICS ===")
    initial = metrics['initial']
    final = metrics['final']
    print(f"DB Size: {initial.get('db_size_mb', 'N/A')}MB -> {final.get('db_size_mb', 'N/A')}MB")
    print(f"Connections: {initial.get('connections', {})}")
    print(f"Pending Locks: {initial.get('pending_locks', 'N/A')} -> {final.get('pending_locks', 'N/A')}")
    print(f"Dead tuples (initial):")
    print(f"  - custody_position: {initial.get('custody_position_dead', 'N/A')}")
    print(f"  - custody_position_buffer: {initial.get('custody_position_buffer_dead', 'N/A')}")
    print(f"Dead tuples (final):")
    print(f"  - custody_position: {final.get('custody_position_dead', 'N/A')}")
    print(f"  - custody_position_buffer: {final.get('custody_position_buffer_dead', 'N/A')}")
    print()
    print("=== ESTIMATED PRODUCTION ===")
    for target in [1000000, 2000000, 4000000]:
        est_time = (target / throughput / 3600) if throughput > 0 else 0
        print(f"For {target:,} records: {est_time:.2f}h ({est_time*60:.1f}min)")
    print("=" * 70)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
