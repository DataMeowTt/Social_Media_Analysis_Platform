from __future__ import annotations
import json
from pyspark.sql import DataFrame, SparkSession

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

def read_silver(spark: SparkSession, dataset: str) -> DataFrame:
    path = f"s3a://{bucket}/processed/{dataset}/"
    logger.info(f"Reading silver from {path}")
    
    return (
        spark.read
        .format("parquet")
        .load(path)
    )