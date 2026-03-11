from pathlib import Path
import pandas as pd


SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")
QUARANTINE_DIR = Path("data/quarantine")

GOLD_DIR.mkdir(parents=True, exist_ok=True)
QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)


def run_data_checks():
    silver_partitions = list(SILVER_DIR.glob("dt=*/normalized_transactions.parquet"))

    if not silver_partitions:
        print("No silver partition files found.")
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

        dt_value = valid_df["dt"].iloc[0] if not valid_df.empty else df["dt"].iloc[0]

        gold_partition_dir = GOLD_DIR / f"dt={dt_value}"
        gold_partition_dir.mkdir(parents=True, exist_ok=True)

        valid_output_path = gold_partition_dir / "validated_transactions.parquet"
        valid_df.to_parquet(valid_output_path, index=False)

        print(f"Saved gold partition: {valid_output_path}")
        print(f"Valid count for {dt_value}: {len(valid_df)}")
        print(f"Invalid count for {dt_value}: {len(invalid_df)}")

        if not invalid_df.empty:
            all_invalid_rows.append(invalid_df)

    if all_invalid_rows:
        final_invalid_df = pd.concat(all_invalid_rows, ignore_index=True)
    else:
        final_invalid_df = pd.DataFrame()

    invalid_output_path = QUARANTINE_DIR / "invalid_transactions.csv"
    final_invalid_df.to_csv(invalid_output_path, index=False)

    print(f"Invalid rows saved to {invalid_output_path}")
    print(f"Total invalid count: {len(final_invalid_df)}")


if __name__ == "__main__":
    run_data_checks()