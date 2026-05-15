from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.utils.logger import get_logger
from src.storage.s3.uploader import write_to_S3   
from src.storage.s3.reader import read_all_bronze, read_latest_bronze
from src.twitter.processing.transformations.clean import clean_tweets
from src.twitter.processing.transformations.enrich import enrich_tweets
from src.twitter.processing.transformations.normalize import flatten_tweets
from src.twitter.processing.validation.quality_checks import validate_tweets

logger = get_logger(__name__)

def historical_processing(spark: SparkSession) -> None:
    logger.info("Starting bronze → silver pipeline")

    df = read_all_bronze(spark, dataset="tweets")
    logger.info("Read complete — transforming...")

    df = flatten_tweets(df)
    df = clean_tweets(df)
    df = enrich_tweets(df)

    df.cache()
    try:
        validate_tweets(df)
        logger.info("Validation passed — writing to silver...")
        write_to_S3(df, table_name="processed.tweets", layer="processed", mode="overwrite")
        logger.info("Silver completed")
    finally:
        df.unpersist()

def incremental_processing(spark: SparkSession) -> None:
    logger.info("Starting bronze → silver pipeline")

    df = read_latest_bronze(spark, dataset="tweets")
    logger.info("Read complete — transforming...")

    df = flatten_tweets(df)
    df = clean_tweets(df)
    df = enrich_tweets(df)

    df.cache()
    try:
        validate_tweets(df)
        logger.info("Validation passed — writing to silver...")
        write_to_S3(df, table_name="processed.tweets", layer="processed", mode="append")
        logger.info("Silver completed")
    finally:
        df.unpersist()