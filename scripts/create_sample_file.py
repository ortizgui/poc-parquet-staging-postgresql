"""Generate a sample Parquet file with ~25 custody position records."""

import os
import pandas as pd


OUTPUT_PATH = "./data/input/custody_position.parquet"


def main():
    records = [
        # --- UPDATES: matching seed keys, different qty/amount ---
        {"account_id": "ACC001", "asset_id": "PETR4", "reference_date": "2025-01-15", "quantity": 1500.0000, "amount": 37500.00},
        {"account_id": "ACC001", "asset_id": "VALE3", "reference_date": "2025-01-15", "quantity": 800.0000, "amount": 24000.00},
        {"account_id": "ACC002", "asset_id": "ITUB4", "reference_date": "2025-01-15", "quantity": 2200.0000, "amount": 77000.00},

        # --- NEW records (unique keys) ---
        {"account_id": "ACC001", "asset_id": "PETR4", "reference_date": "2025-01-16", "quantity": 1600.0000, "amount": 40000.00},
        {"account_id": "ACC001", "asset_id": "VALE3", "reference_date": "2025-01-16", "quantity": 600.0000, "amount": 18000.00},
        {"account_id": "ACC001", "asset_id": "BBAS3", "reference_date": "2025-01-15", "quantity": 3000.0000, "amount": 90000.00},
        {"account_id": "ACC001", "asset_id": "BBAS3", "reference_date": "2025-01-16", "quantity": 3100.0000, "amount": 93000.00},
        {"account_id": "ACC002", "asset_id": "PETR4", "reference_date": "2025-01-15", "quantity": 500.0000, "amount": 12500.00},
        {"account_id": "ACC002", "asset_id": "PETR4", "reference_date": "2025-01-16", "quantity": 550.0000, "amount": 13750.00},
        {"account_id": "ACC002", "asset_id": "VALE3", "reference_date": "2025-01-15", "quantity": 700.0000, "amount": 21000.00},
        {"account_id": "ACC002", "asset_id": "VALE3", "reference_date": "2025-01-16", "quantity": 750.0000, "amount": 22500.00},
        {"account_id": "ACC003", "asset_id": "WEGE3", "reference_date": "2025-01-15", "quantity": 1200.0000, "amount": 36000.00},
        {"account_id": "ACC003", "asset_id": "WEGE3", "reference_date": "2025-01-16", "quantity": 1250.0000, "amount": 37500.00},
        {"account_id": "ACC003", "asset_id": "ABEV3", "reference_date": "2025-01-15", "quantity": 10000.0000, "amount": 150000.00},
        {"account_id": "ACC003", "asset_id": "ABEV3", "reference_date": "2025-01-16", "quantity": 10200.0000, "amount": 153000.00},
        {"account_id": "ACC004", "asset_id": "MGLU3", "reference_date": "2025-01-15", "quantity": 5000.0000, "amount": 25000.00},
        {"account_id": "ACC004", "asset_id": "MGLU3", "reference_date": "2025-01-16", "quantity": 5200.0000, "amount": 26000.00},
        {"account_id": "ACC004", "asset_id": "BBDC4", "reference_date": "2025-01-15", "quantity": 800.0000, "amount": 12000.00},
        {"account_id": "ACC004", "asset_id": "BBDC4", "reference_date": "2025-01-16", "quantity": 820.0000, "amount": 12300.00},
        {"account_id": "ACC005", "asset_id": "ITUB4", "reference_date": "2025-01-15", "quantity": 3500.0000, "amount": 122500.00},

        # --- INVALID records ---
        {"account_id": "ACC006", "asset_id": "INVL1", "reference_date": "2025-01-15", "quantity": -100.0000, "amount": 5000.00},
        {"account_id": "",           "asset_id": "INVL2", "reference_date": "2025-01-15", "quantity": 200.0000, "amount": 6000.00},
        {"account_id": "ACC007", "asset_id": "",           "reference_date": "2025-01-15", "quantity": 300.0000, "amount": 7000.00},
        {"account_id": "ACC008", "asset_id": "INVL4", "reference_date": "2025-01-15", "quantity": 400.0000, "amount": -500.00},
    ]

    df = pd.DataFrame(records)
    df["reference_date"] = pd.to_datetime(df["reference_date"])
    df["quantity"] = df["quantity"].astype("float64")
    df["amount"] = df["amount"].astype("float64")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_parquet(OUTPUT_PATH, engine="pyarrow", index=False)

    print(f"Arquivo gerado: {OUTPUT_PATH}")
    print(f"Linhas: {len(df)}")


if __name__ == "__main__":
    main()
