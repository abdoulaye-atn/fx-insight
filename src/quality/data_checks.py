from pathlib import Path
import pandas as pd


PROCESSED_DIR = Path("data/processed")
QUARANTINE_DIR = Path("data/quarantine")

QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)


def run_data_checks():
    input_path = PROCESSED_DIR / "normalized_transactions.csv"
    valid_output_path = PROCESSED_DIR / "validated_transactions.csv"
    invalid_output_path = QUARANTINE_DIR / "invalid_transactions.csv"

    df = pd.read_csv(input_path)

    invalid_mask = (
        df["rate_to_cad"].isna()
        | df["amount"].isna()
        | df["currency"].isna()
        | df["amount_cad"].isna()
        | (df["amount"] <= 0)
    )

    invalid_df = df[invalid_mask].copy()
    valid_df = df[~invalid_mask].copy()

    valid_df.to_csv(valid_output_path, index=False)
    invalid_df.to_csv(invalid_output_path, index=False)

    print(f"Valid rows saved to {valid_output_path}")
    print(f"Invalid rows saved to {invalid_output_path}")
    print(f"Valid count: {len(valid_df)}")
    print(f"Invalid count: {len(invalid_df)}")


if __name__ == "__main__":
    run_data_checks()