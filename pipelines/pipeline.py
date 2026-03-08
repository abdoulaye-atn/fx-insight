from src.ingestion.generate_sample_data import main as generate_sample_data
from src.processing.normalize_transactions import normalize_transactions
from src.quality.data_checks import run_data_checks


def run_pipeline():
    print("Starting pipeline...")

    print("\nStep 1: Generating sample data...")
    generate_sample_data()

    print("\nStep 2: Normalizing transactions...")
    normalize_transactions()

    print("\nStep 3: Running data quality checks...")
    run_data_checks()

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()