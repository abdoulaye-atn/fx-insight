from pathlib import Path
import pandas as pd


RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def normalize_transactions():

    transactions_path = RAW_DIR / "transactions.csv"
    fx_rates_path = RAW_DIR / "fx_rates.csv"

    transactions = pd.read_csv(transactions_path)
    fx_rates = pd.read_csv(fx_rates_path)

    merged = transactions.merge(
        fx_rates,
        left_on=["transaction_date", "currency"],
        right_on=["rate_date", "currency"],
        how="left"
    )

    merged["amount_cad"] = merged["amount"] * merged["rate_to_cad"]

    output_path = PROCESSED_DIR / "normalized_transactions.csv"

    merged.to_csv(output_path, index=False)

    print(f"Normalized transactions saved to {output_path}")

if __name__ == "__main__":
    normalize_transactions()