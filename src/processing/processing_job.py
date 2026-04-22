from pyspark.sql import DataFrame, SparkSession

from src.utils.logger import get_logger
from src.utils.config_loader import load_config
from src.storage.s3.uploader import upload_to_silver_s3
from src.processing.transformations.clean import clean_tweets
from src.processing.transformations.enrich import enrich_tweets
from src.processing.transformations.normalize import flatten_tweets
from src.processing.validation.quality_checks import validate_tweets
from src.processing.validation.schema import TWEET_SCHEMA

from pyspark.sql import functions as F

logger = get_logger(__name__)
config = load_config()

def read_tweets_from_bronze(spark: SparkSession, year: str, month: str, day: str, bucket: str = config["s3"]["bucket_name"]) -> DataFrame:
    path = f"s3a://{bucket}/raw/tweets/year={year}/month={month}/day={day}/"
    logger.info(f"Reading tweets from {path}")
    
    df = (spark.read
          .option("compression", "gzip")
          .schema(TWEET_SCHEMA) 
          .json(path))
        
    logger.info(f"Read {df.count()} tweets from {path}")
    
    return df
    
def run(spark: SparkSession, year: str, month: str, day: str) -> None:
    df = read_tweets_from_bronze(spark, year, month, day)
    df = flatten_tweets(df)
    df = clean_tweets(df)
    df = enrich_tweets(df)
    df.cache()
    validate_tweets(df)
    upload_to_silver_s3(df, dataset="tweets")
    df.unpersist()
    
# TODO delete later, temporary for first time run to backfill data
def run_backfill(spark: SparkSession) -> None:
    bucket = config["s3"]["bucket_name"]
    path = f"s3a://{bucket}/raw/tweets/"
    logger.info(f"Backfill: reading all bronze data from {path}")

    df = (
        spark.read
        .option("compression", "gzip")
        .schema(TWEET_SCHEMA)
        .json(path)
    )

    df = flatten_tweets(df)
    df = clean_tweets(df)
    df = enrich_tweets(df)

    df = (
        df
        .withColumn("year",  F.year("created_at_ts").cast("string"))
        .withColumn("month", F.month("created_at_ts").cast("string"))
        .withColumn("day",   F.dayofmonth("created_at_ts").cast("string"))
    )

    df.cache()
    validate_tweets(df)

    silver_path = f"s3a://{bucket}/processed/tweets/"
    (
        df.write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .option("compression", "snappy")
        .parquet(silver_path)
    )

    df.unpersist()
    logger.info("Backfill complete")
