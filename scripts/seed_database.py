"""Seed custody_position table with realistic test data.

Usage:
    python scripts/seed_database.py --records 100000
"""

import argparse
import uuid
from datetime import datetime, timedelta
import random

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

ACCOUNTS = [f"ACC{i:03d}" for i in range(1, 10001)]
ASSETS = ["PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3", "PERM4", "RENT3", "RADL3", "HAPV3", "WEGE3",
          "CCRO3", "EMBR3", "GGBR4", "CSNA3", "USIM5", "GOAU4", "BRAP4", "VALE5", "FIBR3", "CPFE3"]


def generate_records(count, days_back=30):
    records = []
    base_date = datetime.now().date()
    for _ in range(count):
        account_id = random.choice(ACCOUNTS)
        asset_id = random.choice(ASSETS)
        reference_date = base_date - timedelta(days=random.randint(0, days_back))
        quantity = round(random.uniform(10, 10000), 4)
        amount = round(random.uniform(100, 1000000), 2)
        records.append((account_id, asset_id, reference_date, quantity, amount))
    return records


def main():
    parser = argparse.ArgumentParser(description="Seed custody_position table")
    parser.add_argument("--records", type=int, default=100000, help="Number of records to generate")
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    cur = conn.cursor()

    print(f"[SEED] Clearing custody_position table...")
    cur.execute("TRUNCATE TABLE custody_position CASCADE")
    conn.commit()

    print(f"[SEED] Generating {args.records} records...")
    records = generate_records(args.records)

    print(f"[SEED] Inserting records in bulk...")
    execute_values(
        cur,
        """INSERT INTO custody_position (account_id, asset_id, reference_date, quantity, amount)
           VALUES %s""",
        records,
        page_size=10000
    )
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM custody_position")
    count = cur.fetchone()[0]
    print(f"[SEED] Done! Table has {count} records.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
