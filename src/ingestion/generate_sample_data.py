from __future__ import annotations

import random
import pandas as pd
from datetime import timedelta
from pathlib import Path
from src.ingestion.fx_api_client import fetch_fx_rates_from_api
from src.utils.config_loader import load_config
from src.utils.logger import get_logger


logger = get_logger(__name__)
config = load_config()

BRONZE_DIR = Path(config["paths"]["bronze"])
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

SAMPLE_TRANSACTION_COUNT = config["pipeline"]["sample_transaction_count"]
FX_START_DATE = config["fx_api"]["start_date"]


def generate_transactions(num_rows: int = 200) -> pd.DataFrame:
    currencies = ["CAD", "USD", "EUR", "GBP"]
    transaction_types = ["purchase", "refund", "salary", "loan_payment", "subscription"]

    start_date = pd.to_datetime(FX_START_DATE)
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


def main() -> None:
    logger.info("Generating sample transactions")
    transactions_df = generate_transactions(num_rows=SAMPLE_TRANSACTION_COUNT)

    logger.info("Fetching FX rates using dedicated API client")
    fx_rates_df = fetch_fx_rates_from_api()

    transactions_output = BRONZE_DIR / "transactions.csv"
    fx_rates_output = BRONZE_DIR / "fx_rates.csv"

    transactions_df.to_csv(transactions_output, index=False)
    fx_rates_df.to_csv(fx_rates_output, index=False)

    logger.info("Generated %s transactions -> %s", len(transactions_df), transactions_output)
    logger.info("Saved %s FX rows -> %s", len(fx_rates_df), fx_rates_output)


if __name__ == "__main__":
    main()