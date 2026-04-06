from pathlib import Path
import pandas as pd
from src.utils.config_loader import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)
config = load_config()

SILVER_DIR = Path(config["paths"]["silver"])
GOLD_DIR = Path(config["paths"]["gold"])
QUARANTINE_DIR = Path(config["paths"]["quarantine"])

GOLD_DIR.mkdir(parents=True, exist_ok=True)
QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)


def run_data_checks():
    silver_partitions = list(SILVER_DIR.glob("dt=*/normalized_transactions.parquet"))

    if not silver_partitions:
        logger.warning("No silver partition files found.")
        return

    all_invalid_rows = []

    for partition_file in silver_partitions:
        df = pd.read_parquet(partition_file)

        invalid_mask = (
            df["rate_to_cad"].isna()
            | df["amount"].isna()
            | df["currency"].isna()
            | df["amount_cad"].isna()
            | (df["amount"] <= 0)
        )

        invalid_df = df[invalid_mask].copy()
        valid_df = df[~invalid_mask].copy()

        dt_folder = partition_file.parent.name
        dt_value = dt_folder.replace("dt=", "")

        gold_partition_dir = GOLD_DIR / f"dt={dt_value}"
        gold_partition_dir.mkdir(parents=True, exist_ok=True)

        valid_output_path = gold_partition_dir / "validated_transactions.parquet"

        valid_df = valid_df.drop(columns=["dt"], errors="ignore")
        valid_df.to_parquet(valid_output_path, index=False)

        logger.info("Saved gold partition: %s", valid_output_path)
        logger.info("Valid count for %s: %s", dt_value, len(valid_df))
        logger.info("Invalid count for %s: %s", dt_value, len(invalid_df))

        if not invalid_df.empty:
            invalid_df["partition_dt"] = dt_value
            all_invalid_rows.append(invalid_df)

    if all_invalid_rows:
        final_invalid_df = pd.concat(all_invalid_rows, ignore_index=True)
    else:
        final_invalid_df = pd.DataFrame()

    invalid_output_path = QUARANTINE_DIR / "invalid_transactions.csv"
    final_invalid_df.to_csv(invalid_output_path, index=False)

    logger.info("Invalid rows saved to %s", invalid_output_path)
    logger.info("Total invalid count: %s", len(final_invalid_df))


if __name__ == "__main__":
    run_data_checks()