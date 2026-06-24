"""Merge PENDING staging records into the final custody_position table in batches.

Uses a two-step approach per batch for production safety:
  1. INSERT records that DO NOT yet exist in the final table
  2. UPDATE records that ALREADY exist in the final table
  3. Mark batch as MERGED (only after steps 1+2 succeed)

This avoids a single monolithic transaction and scales to millions of records.
"""

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "pocdb")
PG_USER = os.getenv("POSTGRES_USER", "pocuser")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pocpass")

BATCH_SIZE = int(os.getenv("MERGE_BATCH_SIZE", "10000"))


def main():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    cur = conn.cursor()

    # Count pending records before merge
    cur.execute("SELECT COUNT(*) FROM custody_position_staging WHERE status = 'PENDING'")
    pending_before = cur.fetchone()[0]
    print(f"Registros pendentes antes: {pending_before}")

    if pending_before == 0:
        print("Nenhum registro pendente para merge.")
        cur.close()
        conn.close()
        return

    total_merged = 0

    # Process in batches — each batch is its own transaction
    while True:
        # Begin implicit transaction + lock next batch of PENDING rows
        cur.execute("""
            SELECT id
            FROM custody_position_staging
            WHERE status = 'PENDING'
            ORDER BY id
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        """, (BATCH_SIZE,))

        batch_rows = cur.fetchall()
        if not batch_rows:
            break

        batch_ids = [row[0] for row in batch_rows]

        # --- Step 1: INSERT new records (not yet in final table) ---
        cur.execute("""
            INSERT INTO custody_position (account_id, asset_id, reference_date, quantity, amount)
            SELECT s.account_id, s.asset_id, s.reference_date, s.quantity, s.amount
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

        # --- Step 2: UPDATE existing records (already in final table) ---
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
        """, (batch_ids,))
        updated = cur.rowcount

        # --- Step 3: Mark batch as merged ---
        cur.execute("""
            UPDATE custody_position_staging
            SET status = 'MERGED', merged_at = NOW()
            WHERE id = ANY(%s)
        """, (batch_ids,))

        conn.commit()
        total_merged += len(batch_ids)
        print(f"  Lote {total_merged}/{pending_before}: "
              f"+{inserted} novos / ~{updated} atualizados (UPDATE inclui recem-inseridos)")

    # Final count
    cur.execute("SELECT COUNT(*) FROM custody_position")
    final_count = cur.fetchone()[0]

    print(f"Registros processados no merge: {total_merged}")
    print(f"Total final na tabela custody_position: {final_count}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
