from pathlib import Path

import boto3

from src.utils.config_loader import load_config
from src.utils.logger import get_logger


logger = get_logger(__name__)
config = load_config()


def upload_file_to_s3(local_path: Path, s3_key: str) -> None:
    bucket = config["s3"]["bucket"]

    logger.info("Uploading %s to s3://%s/%s", local_path, bucket, s3_key)

    s3 = boto3.client("s3")
    s3.upload_file(str(local_path), bucket, s3_key)

    logger.info("Upload completed for s3://%s/%s", bucket, s3_key)


def upload_directory_files_to_s3(local_dir: Path, s3_prefix: str, pattern: str = "*") -> None:
    for file_path in local_dir.rglob(pattern):
        if file_path.is_file():
            relative_path = file_path.relative_to(local_dir)
            s3_key = f"{s3_prefix}/{relative_path.as_posix()}"
            upload_file_to_s3(file_path, s3_key)