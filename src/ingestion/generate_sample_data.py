from __future__ import annotations

import random
from datetime import timedelta
from pathlib import Path
import pandas as pd
from faker import Faker
from src.ingestion.fx_api_client import fetch_fx_rates_from_api
from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.utils.s3_client import upload_file_to_s3

logger = get_logger(__name__)
config = load_config()

BRONZE_DIR = Path(config["paths"]["bronze"])
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

SAMPLE_TRANSACTION_COUNT = config["pipeline"]["sample_transaction_count"]
SAMPLE_CUSTOMER_COUNT = config["pipeline"]["sample_customer_count"]
FX_START_DATE = config["fx_api"]["start_date"]

fake = Faker("fr_CA")


def generate_customers(num_customers: int = 20) -> pd.DataFrame:
    rows = []

    for i in range(num_customers):
        customer_id = 1000 + i
        first_name = fake.first_name()
        last_name = fake.last_name()

        rows.append(
            {
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": fake.email(),
            }
        )

    return pd.DataFrame(rows)


def generate_transactions(num_rows: int = 200, customer_ids: list[int] | None = None) -> pd.DataFrame:
    currencies = ["CAD", "USD", "EUR", "GBP"]
    transaction_types = ["purchase", "refund", "salary", "loan_payment", "subscription"]

    start_date = pd.to_datetime(FX_START_DATE)
    rows = []

    if not customer_ids:
        customer_ids = list(range(1000, 1020))

    for i in range(1, num_rows + 1):
        tx_date = start_date + timedelta(days=random.randint(0, 29))
        currency = random.choice(currencies)

        rows.append(
            {
                "transaction_id": i,
                "customer_id": random.choice(customer_ids),
                "transaction_date": tx_date.strftime("%Y-%m-%d"),
                "amount": round(random.uniform(10, 5000), 2),
                "currency": currency,
                "transaction_type": random.choice(transaction_types),
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    logger.info("Generating sample customers")
    customers_df = generate_customers(num_customers=SAMPLE_CUSTOMER_COUNT)

    logger.info("Generating sample transactions")
    transactions_df = generate_transactions(
        num_rows=SAMPLE_TRANSACTION_COUNT,
        customer_ids=customers_df["customer_id"].tolist(),
    )

    logger.info("Fetching FX rates using dedicated API client")
    fx_rates_df = fetch_fx_rates_from_api()

    customers_output = BRONZE_DIR / "customers.csv"
    transactions_output = BRONZE_DIR / "transactions.csv"
    fx_rates_output = BRONZE_DIR / "fx_rates.csv"

    customers_df.to_csv(customers_output, index=False)
    transactions_df.to_csv(transactions_output, index=False)
    fx_rates_df.to_csv(fx_rates_output, index=False)

    logger.info("Generated %s customers -> %s", len(customers_df), customers_output)
    logger.info("Generated %s transactions -> %s", len(transactions_df), transactions_output)
    logger.info("Saved %s FX rows -> %s", len(fx_rates_df), fx_rates_output)

    storage_type = config["storage"]["type"]

    if storage_type == "s3":
        bronze_prefix = config["s3"]["bronze_prefix"]

        upload_file_to_s3(
            customers_output,
            f"{bronze_prefix}/customers.csv"
        )
        upload_file_to_s3(
            transactions_output,
            f"{bronze_prefix}/transactions.csv"
        )
        upload_file_to_s3(
            fx_rates_output,
            f"{bronze_prefix}/fx_rates.csv"
        )


if __name__ == "__main__":
    main()