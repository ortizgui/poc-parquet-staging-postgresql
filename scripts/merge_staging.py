"""Merge PENDING staging records into the final custody_position table."""

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "pocdb")
PG_USER = os.getenv("POSTGRES_USER", "pocuser")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pocpass")


def main():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM custody_position_staging WHERE status = 'PENDING'")
    pending_before = cur.fetchone()[0]
    print(f"Registros pendentes antes: {pending_before}")

    if pending_before == 0:
        print("Nenhum registro pendente para merge.")
        cur.close()
        conn.close()
        return

    cur.execute("""
        WITH merged AS (
            INSERT INTO custody_position (account_id, asset_id, reference_date, quantity, amount)
            SELECT account_id, asset_id, reference_date, quantity, amount
            FROM custody_position_staging
            WHERE status = 'PENDING'
            ON CONFLICT (account_id, asset_id, reference_date)
            DO UPDATE SET
                quantity = EXCLUDED.quantity,
                amount = EXCLUDED.amount,
                updated_at = NOW()
            RETURNING 1
        )
        UPDATE custody_position_staging
        SET status = 'MERGED', merged_at = NOW()
        WHERE status = 'PENDING'
    """)

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM custody_position")
    final_count = cur.fetchone()[0]

    print(f"Registros processados no merge: {pending_before}")
    print(f"Total final na tabela custody_position: {final_count}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
