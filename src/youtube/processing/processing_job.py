from pyspark.sql import SparkSession

from src.utils.logger import get_logger
from src.storage.s3.uploader import write_to_s3
from src.storage.s3.reader import read_all_bronze
from src.youtube.processing.transformations.clean import clean_comments
from src.youtube.processing.transformations.enrich import enrich_comments
from src.youtube.processing.validation.quality_checks import validate_comments

logger = get_logger(__name__)


def processing(spark: SparkSession, youtube_id: str) -> None:
    logger.info(f"Starting YouTube bronze → silver pipeline for {youtube_id}")

    df = read_all_bronze(spark, dataset="youtube", youtube_id=youtube_id)
    logger.info("Read complete — transforming...")

    df = clean_comments(df)
    df = enrich_comments(df)

    df.cache()
    try:
        validate_comments(df)
        logger.info("Validation passed — writing to silver...")
        write_to_s3(
            df,
            table_name="processed.youtube_comments",
            layer="processed",
            mode="overwrite",
            partition_cols=["year", "month"],
            path_override=f"processed/youtube/{youtube_id}",
        )
        logger.info("Silver completed")
    finally:
        df.unpersist()
