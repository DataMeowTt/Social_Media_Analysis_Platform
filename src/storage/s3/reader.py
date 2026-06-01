from __future__ import annotations
import json
from datetime import date, timedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.utils.session import get_s3_client
from src.twitter.processing.validation.schema import TWEET_SCHEMA
from src.youtube.processing.validation.schema import COMMENT_SCHEMA
from src.facebook.processing.validation.schema import FB_POST_SCHEMA
from src.storage.s3.partitioning import get_latest_partition_prefix

_SCHEMAS = {
    "tweets": TWEET_SCHEMA,
    "youtube": COMMENT_SCHEMA,
    "facebook": FB_POST_SCHEMA,
}

logger = get_logger(__name__)
config = load_config()
bucket = config["s3"]["bucket_name"]

def download_from_s3(s3_key: str) -> list[dict]:
    s3 = get_s3_client()
    response = s3.get_object(Bucket=bucket, Key=s3_key)
    content = response["Body"].read().decode("utf-8")
    
    return json.loads(content)

def read_all_bronze(spark: SparkSession, dataset: str, youtube_id: str | None = None) -> DataFrame:
    if dataset == "youtube":
        if not youtube_id:
            raise ValueError("youtube_id is required for dataset='youtube'")
        path = f"s3a://{bucket}/raw/youtube/{youtube_id}/"
    else:
        path = f"s3a://{bucket}/raw/{dataset}/"
    logger.info(f"Reading all bronze from {path}")
    return spark.read.option("compression", "gzip").schema(_SCHEMAS[dataset]).json(path)


def read_latest_bronze(spark: SparkSession, dataset: str, youtube_id: str | None = None) -> DataFrame:
    if dataset == "youtube":
        if not youtube_id:
            raise ValueError("youtube_id is required for dataset='youtube'")
        path = f"s3a://{bucket}/raw/youtube/{youtube_id}/"
    else:
        path = get_latest_partition_prefix(dataset=dataset, layer="raw")
    logger.info(f"Reading latest bronze from {path}")
    return spark.read.option("compression", "gzip").schema(_SCHEMAS[dataset]).json(path)

def read_all_silver(spark: SparkSession, dataset: str) -> DataFrame:
    path = f"s3a://{bucket}/processed/{dataset}/"
    logger.info(f"Reading all silver data from {path}")

    return (
        spark.read
        .format("parquet")
        .load(path)
    )

def read_fact_tweets_window(spark: SparkSession, days: int) -> DataFrame:
    cutoff = date.today() - timedelta(days=days)
    path = f"s3a://{bucket}/analytics/tweets/fact_tweets/"
    logger.info(f"Reading fact_tweets: last {days} days (>= {cutoff}) from {path}")
    return spark.read.format("parquet").load(path).filter(F.col("date") >= F.lit(cutoff))


def read_latest_silver_facebook(spark: SparkSession, table: str) -> DataFrame:
    today = date.today()
    path = f"s3a://{bucket}/processed/facebook/{table}/ingestion_date={today}/"
    logger.info(f"Reading today's facebook silver from {path}")
    return spark.read.format("parquet").load(path)


def read_latest_silver(spark: SparkSession, dataset: str) -> DataFrame:
    path = get_latest_partition_prefix(dataset=dataset, layer="processed")
    logger.info(f"Reading latest silver from {path}")
    return spark.read.format("parquet").load(path)


def read_silver_youtube(spark: SparkSession, youtube_id: str) -> DataFrame:
    path = f"s3a://{bucket}/processed/youtube/{youtube_id}/"
    logger.info(f"Reading YouTube silver from {path}")
    return spark.read.format("parquet").load(path)
