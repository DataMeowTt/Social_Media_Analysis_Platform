from __future__ import annotations
import json
from datetime import date, timedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.utils.session import get_s3_client
from src.processing.validation.schema import TWEET_SCHEMA
from src.storage.s3.partitioning import get_latest_partition_prefix

logger = get_logger(__name__)
config = load_config()
bucket = config["s3"]["bucket_name"]

def download_from_s3(s3_key: str) -> list[dict]:
    s3 = get_s3_client()
    response = s3.get_object(Bucket=bucket, Key=s3_key)
    content = response["Body"].read().decode("utf-8")
    
    return json.loads(content)

def read_all_bronze(spark: SparkSession, dataset: str) -> DataFrame:
    path = f"s3a://{bucket}/raw/{dataset}/"
    logger.info(f"Reading all data from {path}")
    
    return (
        spark.read
        .option("compression", "gzip")
        .schema(TWEET_SCHEMA)
        .json(path)
    )

def read_latest_bronze(spark: SparkSession, dataset: str) -> DataFrame:
    path = get_latest_partition_prefix(dataset=dataset, layer="raw")
    logger.info(f"Reading latest partition from {path}")

    return (
        spark.read
        .option("compression", "gzip")
        .schema(TWEET_SCHEMA)
        .json(path)
    )

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
    path = f"s3a://{bucket}/analytics/fact_tweets/"
    logger.info(f"Reading fact_tweets: last {days} days (>= {cutoff}) from {path}")
    return spark.read.format("parquet").load(path).filter(F.col("date") >= F.lit(cutoff))


def read_latest_silver(spark: SparkSession, dataset: str) -> DataFrame:
    today = date.today()
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    paths = [
        f"s3a://{bucket}/{obj['Key']}"
        for page in paginator.paginate(Bucket=bucket, Prefix=f"processed/{dataset}/")
        for obj in page.get("Contents", [])
        if obj["LastModified"].date() == today
    ]

    if not paths:
        raise ValueError(f"No silver files modified today ({today}) for dataset '{dataset}'")

    logger.info(f"Reading {len(paths)} silver file(s) modified today ({today})")
    return spark.read.format("parquet").load(paths)