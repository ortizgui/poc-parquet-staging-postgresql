"""Load simulation tool with concurrent operations and metrics collection.

Usage:
    python3 scripts/simulate_load.py --existing-records 100000 --ingestion-size 10000 --update-ratio 60 --concurrent-ops 10
"""

import argparse
import uuid
import random
import time
import threading
import csv
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Aurora Graviton 6 specs for estimation
AURORA_R6G_LARGE = {'vcpus': 2, 'ram_gb': 16, 'name': 'r6g.large'}
AURORA_R6G_XLARGE = {'vcpus': 4, 'ram_gb': 32, 'name': 'r6g.xlarge'}
LOCAL_SPECS = {'vcpus': 10, 'ram_gb': 32, 'name': 'Mac M4 Pro'}  # Approximate


class ConcurrentOperationsSimulator:
    """Simulates other operations running during merge."""
    
    def __init__(self, ops_per_second=10, duration_seconds=60):
        self.ops_per_second = ops_per_second
        self.duration_seconds = duration_seconds
        self.results = []
        self.errors = []
        self.running = False
        self.thread = None
        
    def start(self, connection_params):
        self.running = True
        self.thread = threading.Thread(target=self._run_operations, args=(connection_params,))
        self.thread.start()
        
    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
            
    def _run_operations(self, conn_params):
        interval = 1.0 / self.ops_per_second if self.ops_per_second > 0 else 1.0
        
        while self.running:
            try:
                conn = psycopg2.connect(**conn_params)
                cur = conn.cursor()
                
                op_type = random.choice(['INSERT', 'UPDATE', 'SELECT'])
                start_time = time.time()
                success = True
                error_msg = None
                
                if op_type == 'INSERT':
                    account = random.choice(ACCOUNTS)
                    asset = random.choice(ASSETS)
                    cur.execute("""
                        INSERT INTO custody_position (account_id, asset_id, reference_date, quantity, amount)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (account, asset, datetime.now().date(), 
                          random.uniform(10, 1000), random.uniform(100, 10000)))
                elif op_type == 'UPDATE':
                    cur.execute("""
                        UPDATE custody_position 
                        SET amount = amount * 1.01
                        WHERE id = (SELECT id FROM custody_position ORDER BY random() LIMIT 1)
                    """)
                else:  # SELECT
                    cur.execute("""
                        SELECT COUNT(*) FROM custody_position 
                        WHERE account_id = %s
                    """, (random.choice(ACCOUNTS),))
                    cur.fetchone()
                
                conn.commit()
                latency_ms = (time.time() - start_time) * 1000
                
                self.results.append({
                    'timestamp': time.time(),
                    'op_type': op_type,
                    'latency_ms': latency_ms,
                    'success': True
                })
                
                cur.close()
                conn.close()
                
            except Exception as e:
                self.errors.append({
                    'timestamp': time.time(),
                    'op_type': op_type,
                    'error': str(e)[:100]
                })
            
            time.sleep(interval)
    
    def get_stats(self):
        if not self.results:
            return {'total': 0, 'errors': len(self.errors), 'avg_latency_ms': 0}
        
        latencies = [r['latency_ms'] for r in self.results]
        return {
            'total': len(self.results),
            'errors': len(self.errors),
            'avg_latency_ms': sum(latencies) / len(latencies),
            'max_latency_ms': max(latencies),
            'p95_latency_ms': sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0
        }


def generate_unique_records(count, days_back=30):
    """Generate records with unique combos."""
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
    """Seed principal table, always truncating first."""
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


def estimate_aurora_time(local_throughput, aurora_specs, local_specs=LOCAL_SPECS):
    """Estimate processing time on Aurora based on local performance."""
    # Rough estimation based on CPU cores ratio
    cpu_ratio = aurora_specs['vcpus'] / local_specs['vcpus']
    ram_ratio = aurora_specs['ram_gb'] / local_specs['ram_gb']
    
    # Weighted average (CPU is more important for this workload)
    performance_ratio = (cpu_ratio * 0.7 + ram_ratio * 0.3)
    
    # Local is faster per core typically, so we adjust
    estimated_throughput = local_throughput * performance_ratio * 0.8  # 0.8 = Mac M4 vs Graviton6 efficiency
    
    return {
        'aurora_specs': aurora_specs,
        'local_specs': local_specs,
        'estimated_throughput': estimated_throughput,
        'cpu_ratio': cpu_ratio,
        'ram_ratio': ram_ratio
    }


def collect_metrics(cursor):
    """Collect comprehensive database metrics."""
    metrics = {}
    
    try:
        # Database size
        cursor.execute("SELECT pg_database_size(%s) / 1024 / 1024", (PG_DB,))
        metrics['db_size_mb'] = cursor.fetchone()[0]
    except:
        metrics['db_size_mb'] = None
    
    try:
        # Table stats
        cursor.execute("""
            SELECT 
                c.relname,
                pg_stat_get_live_tup(c.oid) as live_tuples,
                pg_stat_get_dead_tup(c.oid) as dead_tuples,
                pg_size_pretty(pg_total_relation_size(c.oid)) as total_size
            FROM pg_class c
            WHERE c.relname IN ('custody_position', 'custody_position_buffer')
            ORDER BY c.relname
        """)
        for row in cursor.fetchall():
            metrics[f'{row[0]}_live'] = row[1]
            metrics[f'{row[0]}_dead'] = row[2]
            metrics[f'{row[0]}_size'] = row[3]
    except Exception as e:
        pass
    
    try:
        # Connections
        cursor.execute("""
            SELECT state, COUNT(*) 
            FROM pg_stat_activity 
            WHERE datname = %s
            GROUP BY state
        """, (PG_DB,))
        metrics['connections'] = dict(cursor.fetchall())
    except:
        metrics['connections'] = {}
    
    try:
        # Lock waiters
        cursor.execute("""
            SELECT COUNT(*) 
            FROM pg_locks 
            WHERE granted = false
        """)
        metrics['pending_locks'] = cursor.fetchone()[0]
    except:
        metrics['pending_locks'] = 0
    
    try:
        # Long running transactions
        cursor.execute("""
            SELECT COUNT(*) 
            FROM pg_stat_activity 
            WHERE state = 'idle in transaction' 
            AND query_start < NOW() - INTERVAL '5 seconds'
        """)
        metrics['long_transactions'] = cursor.fetchone()[0]
    except:
        metrics['long_transactions'] = 0
    
    try:
        # Buffer cache hit ratio
        cursor.execute("""
            SELECT 
                CASE WHEN blks_hit + blks_read = 0 THEN 0
                ELSE ROUND(100.0 * blks_hit / (blks_hit + blks_read), 2)
                END as cache_hit_ratio
            FROM pg_stat_database 
            WHERE datname = %s
        """, (PG_DB,))
        metrics['cache_hit_ratio'] = cursor.fetchone()[0]
    except:
        metrics['cache_hit_ratio'] = None
    
    return metrics


def run_merge_with_metrics(cursor, conn, batch_size, delay, csv_writer=None):
    """Run merge with detailed metrics collection."""
    start_time = time.time()
    
    cursor.execute("SELECT pg_advisory_lock(%s)", (MERGE_LOCK_ID,))
    print("[MERGE] Advisory lock acquired (lock_id=42)")
    
    cursor.execute("SELECT COUNT(*) FROM custody_position_buffer WHERE status = 'PENDING'")
    pending_before = cursor.fetchone()[0]
    
    if pending_before == 0:
        cursor.execute("SELECT pg_advisory_unlock(%s)", (MERGE_LOCK_ID,))
        return 0, [], 0, {}
    
    total_merged = 0
    batch_results = []
    metrics_history = []
    
    while True:
        batch_start = time.time()
        batch_num = total_merged // batch_size + 1
        
        # Collect pre-batch metrics
        pre_metrics = collect_metrics(cursor)
        
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
        
        batch_time = time.time() - batch_start
        total_merged += len(batch_ids)
        
        # Collect post-batch metrics
        post_metrics = collect_metrics(cursor)
        post_metrics['timestamp'] = time.time() - start_time
        post_metrics['batch'] = batch_num
        post_metrics['batch_time'] = batch_time
        post_metrics['inserted'] = inserted
        post_metrics['updated'] = updated
        post_metrics['total_processed'] = total_merged
        
        metrics_history.append(post_metrics)
        
        # Write to CSV if provided
        if csv_writer:
            csv_writer.writerow({
                'timestamp': post_metrics['timestamp'],
                'batch': batch_num,
                'batch_time_ms': batch_time * 1000,
                'inserted': inserted,
                'updated': updated,
                'total_processed': total_merged,
                'pending_locks': post_metrics.get('pending_locks', 0),
                'dead_custody': post_metrics.get('custody_position_dead', 0),
                'dead_buffer': post_metrics.get('custody_position_buffer_dead', 0),
                'cache_hit_ratio': post_metrics.get('cache_hit_ratio', 0),
                'long_transactions': post_metrics.get('long_transactions', 0)
            })
        
        pct = (total_merged / pending_before) * 100
        total_batches = (pending_before + batch_size - 1) // batch_size
        
        print(f"  [BATCH {batch_num}/{total_batches}] +{inserted}ins ~{updated}upd | "
              f"time={batch_time*1000:.0f}ms | "
              f"locks={post_metrics.get('pending_locks', 0)} | "
              f"dead_cp={post_metrics.get('custody_position_dead', 0)} | "
              f"dead_buf={post_metrics.get('custody_position_buffer_dead', 0)} | "
              f"cache={post_metrics.get('cache_hit_ratio', 'N/A')}% | "
              f"{pct:.0f}%")
        
        if delay > 0:
            time.sleep(delay)
    
    cursor.execute("SELECT pg_advisory_unlock(%s)", (MERGE_LOCK_ID,))
    print("[MERGE] Advisory lock released")
    
    cursor.execute("DELETE FROM custody_position_buffer WHERE status = 'MERGED'")
    cursor.connection.commit()
    
    total_time = time.time() - start_time
    
    return total_merged, batch_results, total_time, metrics_history


def main():
    parser = argparse.ArgumentParser(description="Load simulation tool with concurrent ops")
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
    parser.add_argument("--concurrent-ops", type=int, default=0,
                        help="Simulate N concurrent operations per second during merge")
    parser.add_argument("--output-csv", type=str, default="",
                        help="Output CSV file for metrics")
    args = parser.parse.parse_args()
    
    conn_params = {
        'host': PG_HOST,
        'port': PG_PORT,
        'dbname': PG_DB,
        'user': PG_USER,
        'password': PG_PASSWORD
    }
    
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    
    seed_principal_table(cursor, args.existing_records)
    clear_buffer_table(cursor)
    
    update_count = int(args.ingestion_size * args.update_ratio / 100)
    insert_count = args.ingestion_size - update_count
    
    print(f"[GEN] Generating {args.ingestion_size} records...")
    
    cursor.execute("""
        SELECT account_id, asset_id, reference_date
        FROM custody_position
        ORDER BY random()
        LIMIT %s
    """, (update_count,))
    existing_combos = set((row[0], row[1], row[2]) for row in cursor.fetchall())
    
    cursor.execute("SELECT account_id, asset_id, reference_date FROM custody_position")
    all_existing_combos = set((row[0], row[1], row[2]) for row in cursor.fetchall())
    planned_insert_combos = set()
    
    base_date = datetime.now().date()
    dates = [base_date - timedelta(days=i) for i in range(31)]
    
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
        if key in all_existing_combos or key in planned_insert_combos:
            continue
        
        planned_insert_combos.add(key)
        quantity = round(random.uniform(10, 10000), 4)
        amount = round(random.uniform(100, 1000000), 2)
        record_hash = uuid.uuid4().hex[:16]
        insert_records.append((batch_uuid, source_file, next_row_number, record_hash,
                            account_id, asset_id, reference_date, quantity, amount, 'PENDING'))
        next_row_number += 1
    
    all_records = update_records + insert_records
    random.shuffle(all_records)
    
    print(f"[GEN] Prepared {len(update_records)} updates + {len(insert_records)} inserts")
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
    print(f"[GEN] Buffer has {cursor.fetchone()[0]} PENDING records")
    
    # Open CSV file if specified
    csv_file = None
    csv_writer = None
    if args.output_csv:
        csv_file = open(args.output_csv, 'w', newline='')
        csv_writer = csv.DictWriter(csv_file, fieldnames=[
            'timestamp', 'batch', 'batch_time_ms', 'inserted', 'updated',
            'total_processed', 'pending_locks', 'dead_custody', 'dead_buffer',
            'cache_hit_ratio', 'long_transactions'
        ])
        csv_writer.writeheader()
        print(f"[CSV] Writing metrics to {args.output_csv}")
    
    # Start concurrent operations simulator if requested
    concurrent_sim = None
    if args.concurrent_ops > 0:
        concurrent_sim = ConcurrentOperationsSimulator(
            ops_per_second=args.concurrent_ops,
            duration_seconds=300  # 5 minutes max
        )
        concurrent_sim.start(conn_params)
        print(f"[CONCURRENT] Simulating {args.concurrent_ops} ops/sec during merge")
    
    # Run merge with metrics
    print(f"\n[MERGE] Starting merge...")
    total_merged, batch_results, total_time, metrics_history = run_merge_with_metrics(
        cursor, conn, args.batch_size, args.delay, csv_writer
    )
    
    # Stop concurrent simulator
    concurrent_stats = None
    if concurrent_sim:
        concurrent_sim.stop()
        concurrent_stats = concurrent_sim.get_stats()
    
    if csv_file:
        csv_file.close()
    
    throughput = total_merged / total_time if total_time > 0 else 0
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print("\n" + "=" * 70)
    print("=== LOAD SIMULATION REPORT ===")
    print(f"Date: {timestamp}")
    print(f"Existing Records: {args.existing_records:,}")
    print(f"Ingestion Size: {args.ingestion_size:,} ({args.update_ratio}% updates)")
    print(f"Batch Size: {args.batch_size}, Delay: {args.delay}s")
    if args.concurrent_ops > 0:
        print(f"Concurrent Ops: {args.concurrent_ops}/sec simulated")
    print()
    print("=== TIMING RESULTS ===")
    print(f"Total Time: {total_time:.2f}s")
    print(f"Throughput: {throughput:.0f} records/second")
    print()
    
    if concurrent_stats:
        print("=== CONCURRENT OPERATIONS IMPACT ===")
        print(f"Total ops attempted: {concurrent_stats['total']}")
        print(f"Errors: {concurrent_stats['errors']}")
        print(f"Avg latency: {concurrent_stats['avg_latency_ms']:.1f}ms")
        print(f"Max latency: {concurrent_stats['max_latency_ms']:.1f}ms")
        print(f"P95 latency: {concurrent_stats['p95_latency_ms']:.1f}ms")
        if concurrent_stats['errors'] > 0:
            error_rate = concurrent_stats['errors'] / (concurrent_stats['total'] + concurrent_stats['errors']) * 100
            print(f"Error rate: {error_rate:.1f}%")
        print()
    
    print("=== ESTIMATED AURORA PERFORMANCE ===")
    for aurora_spec in [AURORA_R6G_LARGE, AURORA_R6G_XLARGE]:
        est = estimate_aurora_time(throughput, aurora_spec)
        for target in [1000000, 4000000]:
            est_time_h = (target / est['estimated_throughput'] / 3600) if est['estimated_throughput'] > 0 else 0
            print(f"{aurora_spec['name']} ({aurora_spec['vcpus']} vCPU, {aurora_spec['ram_gb']}GB): "
                  f"{target:,} regs → {est_time_h:.2f}h ({est_time_h*60:.0f}min)")
    print()
    
    print("=== FINAL DATABASE STATE ===")
    final_metrics = collect_metrics(cursor)
    print(f"DB Size: {final_metrics.get('db_size_mb', 'N/A')}MB")
    print(f"Table sizes:")
    print(f"  - custody_position: {final_metrics.get('custody_position_size', 'N/A')} "
          f"(dead: {final_metrics.get('custody_position_dead', 'N/A')})")
    print(f"  - custody_position_buffer: {final_metrics.get('custody_position_buffer_size', 'N/A')} "
          f"(dead: {final_metrics.get('custody_position_buffer_dead', 'N/A')})")
    print(f"Connections: {final_metrics.get('connections', {})}")
    print(f"Pending locks: {final_metrics.get('pending_locks', 'N/A')}")
    print(f"Cache hit ratio: {final_metrics.get('cache_hit_ratio', 'N/A')}%")
    print("=" * 70)
    
    if args.output_csv:
        print(f"\n[CSV] Metrics saved to {args.output_csv}")
        print("[CSV] To generate chart, use:")
        print(f"  python3 -c \"import pandas as pd; df=pd.read_csv('{args.output_csv}'); "
              "df.plot(x='timestamp', y=['pending_locks', 'dead_custody'])\"")
    
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
