from src.ingestion.generate_sample_data import main as generate_sample_data
from src.processing.normalize_transactions import normalize_transactions
from src.quality.data_checks import run_data_checks
from src.utils.logger import get_logger

logger = get_logger(__name__)

def run_pipeline():

    logger.info("Starting pipeline")

    logger.info("Step 1: Generating sample data")
    generate_sample_data()

    logger.info("Step 2: Normalizing transactions")
    normalize_transactions()

    logger.info("Step 3: Running data quality checks")
    run_data_checks()

    logger.info("Pipeline completed successfully")

if __name__ == "__main__":
    run_pipeline()