from pathlib import Path
import pandas as pd
from src.utils.config_loader import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)
config = load_config()

BRONZE_DIR = Path(config["paths"]["bronze"])
SILVER_DIR = Path(config["paths"]["silver"])

SILVER_DIR.mkdir(parents=True, exist_ok=True)


def normalize_transactions():
    customers_path = BRONZE_DIR / "customers.csv"
    transactions_path = BRONZE_DIR / "transactions.csv"
    fx_rates_path = BRONZE_DIR / "fx_rates.csv"

    customers = pd.read_csv(customers_path)
    transactions = pd.read_csv(transactions_path)
    fx_rates = pd.read_csv(fx_rates_path)

    merged = transactions.merge(
        fx_rates,
        left_on=["transaction_date", "currency"],
        right_on=["rate_date", "currency"],
        how="left"
    )

    merged = merged.merge(
        customers,
        on="customer_id",
        how="left"
    )

    merged["amount_cad"] = merged["amount"] * merged["rate_to_cad"]
    merged["dt"] = merged["transaction_date"]

    for dt_value, partition_df in merged.groupby("dt"):
        partition_dir = SILVER_DIR / f"dt={dt_value}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        output_path = partition_dir / "normalized_transactions.parquet"

        partition_df = partition_df.drop(columns=["dt"], errors="ignore")
        partition_df.to_parquet(output_path, index=False)

        logger.info("Saved silver partition: %s", output_path)


if __name__ == "__main__":
    normalize_transactions()