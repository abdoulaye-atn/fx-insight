from pathlib import Path
import pandas as pd


def main():
    gold_path = Path("data/gold")
    files = list(gold_path.glob("dt=*/validated_transactions.parquet"))

    if not files:
        raise FileNotFoundError(
            "No parquet files found in data/gold. Run the pipeline first."
        )

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    if len(df) > 1000:
        df = df.sample(n=1000, random_state=42).sort_values("transaction_date")

    demo_dir = Path("demo_data")
    demo_dir.mkdir(parents=True, exist_ok=True)

    output_path = demo_dir / "gold_sample.parquet"
    df.to_parquet(output_path, index=False)

    print(f"Demo dataset created: {output_path}")
    print(f"Rows: {len(df)}")


if __name__ == "__main__":
    main()