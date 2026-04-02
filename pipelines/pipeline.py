from pathlib import Path

from src.ingestion.generate_sample_data import main as generate_sample_data
from src.processing.normalize_transactions import normalize_transactions
from src.quality.data_checks import run_data_checks
from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.utils.s3_client import upload_directory_files_to_s3, upload_file_to_s3


logger = get_logger(__name__)
config = load_config()


def run_pipeline():
    logger.info("Starting pipeline")

    logger.info("Step 1: Generating sample data")
    generate_sample_data()

    logger.info("Step 2: Normalizing transactions")
    normalize_transactions()

    logger.info("Step 3: Running data quality checks")
    run_data_checks()

    storage_type = config["storage"]["type"]

    if storage_type == "s3":
        logger.info("Step 4: Uploading Silver, Gold and Quarantine to S3")

        silver_dir = Path(config["paths"]["silver"])
        gold_dir = Path(config["paths"]["gold"])
        quarantine_dir = Path(config["paths"]["quarantine"])

        silver_prefix = config["s3"]["silver_prefix"]
        gold_prefix = config["s3"]["gold_prefix"]
        quarantine_prefix = config["s3"]["quarantine_prefix"]

        upload_directory_files_to_s3(silver_dir, silver_prefix, pattern="*.parquet")
        upload_directory_files_to_s3(gold_dir, gold_prefix, pattern="*.parquet")

        invalid_file = quarantine_dir / "invalid_transactions.csv"
        if invalid_file.exists():
            upload_file_to_s3(
                invalid_file,
                f"{quarantine_prefix}/invalid_transactions.csv"
            )

    logger.info("Pipeline completed successfully")

if __name__ == "__main__":
    run_pipeline()