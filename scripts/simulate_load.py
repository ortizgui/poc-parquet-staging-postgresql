"""Load simulation tool for the POC.

Usage:
    python scripts/simulate_load.py --existing-records 100000 --ingestion-size 10000 --update-ratio 60
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


def generate_existing_combos(cursor, count):
    cursor.execute("""
        SELECT account_id, asset_id, reference_date
        FROM custody_position
        ORDER BY random()
        LIMIT %s
    """, (count,))
    return cursor.fetchall()


def generate_unique_records_for_seed(count, days_back=30):
    """Generate records with unique (account_id, asset_id, reference_date) combos."""
    records = []
    base_date = datetime.now().date()
    dates = [base_date - timedelta(days=i) for i in range(days_back + 1)]
    
    seen = set()
    attempts = 0
    max_attempts = count * 2
    
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


def generate_insert_records(count, days_back=30):
    """Generate new records for insert that won't conflict with existing data."""
    records = []
    base_date = datetime.now().date()
    source_file = f"sim_{uuid.uuid4().hex[:8]}.parquet"
    
    seen = set()
    attempts = 0
    max_attempts = count * 2
    
    while len(records) < count and attempts < max_attempts:
        attempts += 1
        account_id = random.choice(ACCOUNTS)
        asset_id = random.choice(ASSETS)
        reference_date = base_date - timedelta(days=random.randint(0, days_back))
        
        key = (account_id, asset_id, reference_date)
        if key in seen:
            continue
            
        seen.add(key)
        quantity = round(random.uniform(10, 10000), 4)
        amount = round(random.uniform(100, 1000000), 2)
        row_number = len(records) + 1
        record_hash = uuid.uuid4().hex[:16]
        records.append((uuid.UUID(source_file), source_file, row_number, record_hash,
                        account_id, asset_id, reference_date, quantity, amount, 'PENDING'))
    
    return records


def seed_principal_table(cursor, count):
    cursor.execute("SELECT COUNT(*) FROM custody_position")
    existing = cursor.fetchone()[0]
    if existing >= count:
        print(f"[SEED] Principal table already has {existing} records (need {count})")
        return

    print(f"[SEED] Seeding principal table with {count} records...")
    cursor.execute("TRUNCATE TABLE custody_position CASCADE")

    records = generate_unique_records_for_seed(count)

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


def run_merge(cursor, conn, batch_size, delay):
    cursor.execute("SELECT pg_advisory_lock(%s)", (MERGE_LOCK_ID,))

    cursor.execute("SELECT COUNT(*) FROM custody_position_buffer WHERE status = 'PENDING'")
    pending_before = cursor.fetchone()[0]

    if pending_before == 0:
        cursor.execute("SELECT pg_advisory_unlock(%s)", (MERGE_LOCK_ID,))
        return 0, [], 0

    total_merged = 0
    batch_results = []
    start_time = time.time()

    while True:
        batch_start = time.time()
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
        batch_results.append((len(batch_ids), inserted, updated, batch_time))

        if delay > 0:
            time.sleep(delay)

    cursor.execute("SELECT pg_advisory_unlock(%s)", (MERGE_LOCK_ID,))

    cursor.execute("DELETE FROM custody_position_buffer WHERE status = 'MERGED'")
    cursor.connection.commit()

    total_time = time.time() - start_time
    return total_merged, batch_results, total_time


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
    parser.add_argument("--accounts", type=int, default=1000,
                        help="Unique accounts to generate")
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    cursor = conn.cursor()

    seed_principal_table(cursor, args.existing_records)
    clear_buffer_table(cursor)

    print(f"[GEN] Generating {args.ingestion_size} records ({args.update_ratio}% updates)...")
    update_count = int(args.ingestion_size * args.update_ratio / 100)
    insert_count = args.ingestion_size - update_count

    # Get existing combos for updates
    existing_combos = generate_existing_combos(cursor, update_count)
    
    # Generate update records (using existing combos)
    source_file = f"sim_{uuid.uuid4().hex[:8]}.parquet"
    update_records = []
    for i, combo in enumerate(existing_combos):
        account_id, asset_id, reference_date = combo
        quantity = round(random.uniform(10, 10000), 4)
        amount = round(random.uniform(100, 1000000), 2)
        record_hash = uuid.uuid4().hex[:16]
        update_records.append((uuid.UUID(source_file), source_file, i + 1, record_hash,
                              account_id, asset_id, reference_date, quantity, amount, 'PENDING'))

    # Generate insert records (new combos)
    insert_records = generate_insert_records(insert_count)

    all_records = update_records + insert_records
    random.shuffle(all_records)

    print(f"[GEN] Inserting {len(all_records)} records into buffer table...")
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

    print(f"[MERGE] Starting merge with batch_size={args.batch_size}, delay={args.delay}s...")
    total_merged, batch_results, total_time = run_merge(cursor, conn, args.batch_size, args.delay)

    throughput = total_merged / total_time if total_time > 0 else 0

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    update_pct = args.update_ratio
    insert_pct = 100 - args.update_ratio

    print("\n" + "=" * 60)
    print("=== LOAD SIMULATION REPORT ===")
    print(f"Date: {timestamp}")
    print(f"Existing Records: {args.existing_records}")
    print(f"Ingestion Size: {args.ingestion_size} ({update_pct}% updates, {insert_pct}% inserts)")
    print(f"Batch Size: {args.batch_size}")
    print(f"Delay: {args.delay}s")
    print()
    print("=== RESULTS ===")
    print(f"Total Time: {total_time:.3f}s")
    print(f"Throughput: {throughput:.1f} records/second")
    print(f"Batches: {len(batch_results)}")
    print("Per-batch breakdown:")
    for i, (count, ins, upd, bt) in enumerate(batch_results, 1):
        print(f"  - Batch {i}: {ins} ins, {upd} upd - {bt:.3f}s")
    print()
    print("=== ESTIMATED PRODUCTION ===")
    for target in [4000000]:
        est_time = (target / throughput / 3600) if throughput > 0 else 0
        print(f"For {target:,} records: {est_time:.1f}h")
    print("=" * 60)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
