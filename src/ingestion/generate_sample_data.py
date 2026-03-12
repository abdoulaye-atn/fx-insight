from __future__ import annotations

import random
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from src.utils.config_loader import load_config

config = load_config()

BRONZE_DIR = Path(config["paths"]["bronze"])
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

SAMPLE_TRANSACTION_COUNT = config["pipeline"]["sample_transaction_count"]

def generate_transactions(num_rows: int = SAMPLE_TRANSACTION_COUNT) -> pd.DataFrame:
    """Generate sample multi-currency financial transactions."""
    currencies = ["CAD", "USD", "EUR", "GBP"]
    transaction_types = ["purchase", "refund", "salary", "loan_payment", "subscription"]

    start_date = datetime(2024, 1, 1)
    rows = []

    for i in range(1, num_rows + 1):
        tx_date = start_date + timedelta(days=random.randint(0, 29))
        currency = random.choice(currencies)

        rows.append(
            {
                "transaction_id": i,
                "customer_id": random.randint(1000, 1015),
                "transaction_date": tx_date.strftime("%Y-%m-%d"),
                "amount": round(random.uniform(10, 5000), 2),
                "currency": currency,
                "transaction_type": random.choice(transaction_types),
            }
        )

    return pd.DataFrame(rows)


def generate_fx_rates() -> pd.DataFrame:
    """Generate sample daily FX rates to CAD for 30 days."""
    base_rates = {
        "CAD": 1.00,
        "USD": 1.35,
        "EUR": 1.47,
        "GBP": 1.72,
    }

    start_date = datetime(2024, 1, 1)
    rows = []

    for day in range(30):
        rate_date = (start_date + timedelta(days=day)).strftime("%Y-%m-%d")

        for currency, base_rate in base_rates.items():
            variation = random.uniform(-0.02, 0.02)
            rate = round(base_rate + variation, 4) if currency != "CAD" else 1.0

            rows.append(
                {
                    "rate_date": rate_date,
                    "currency": currency,
                    "rate_to_cad": rate,
                }
            )

    return pd.DataFrame(rows)


def main() -> None:
    transactions_df = generate_transactions(num_rows=SAMPLE_TRANSACTION_COUNT)
    fx_rates_df = generate_fx_rates()

    transactions_output = BRONZE_DIR / "transactions.csv"
    fx_rates_output = BRONZE_DIR / "fx_rates.csv"

    transactions_df.to_csv(transactions_output, index=False)
    fx_rates_df.to_csv(fx_rates_output, index=False)

    print(f"Generated {len(transactions_df)} transactions -> {transactions_output}")
    print(f"Generated {len(fx_rates_df)} FX rates -> {fx_rates_output}")


if __name__ == "__main__":
    main()