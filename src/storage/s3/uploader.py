from __future__ import annotations
import json
import gzip
from datetime import datetime, timezone

from src.storage.s3.partitioning import get_s3_key, get_partition_prefix
from src.utils.config_loader import load_config, load_table_config
from src.utils.logger import get_logger
from src.utils.aws_session import get_s3_client
from pyspark.sql import DataFrame

logger = get_logger(__name__)
config = load_config()
bucket = config["s3"]["bucket_name"]

def upload_to_bronze_s3(
    data: list[dict], 
    dataset: str, 
    ingestion_time: datetime = None
) -> str:
    table_config = load_table_config("raw")

    if ingestion_time is None:
        ingestion_time = datetime.now(timezone.utc)
        
    filename = f"{dataset}_{ingestion_time.strftime('%Y%m%d_%H%M%S')}.json.gz"
    s3_key = get_s3_key(
        layer="raw",
        dataset=dataset,
        filename=filename,
        partition_time=ingestion_time,
        partition_cols=table_config["partition_cols"]
    )

    s3 = get_s3_client()
    body = gzip.compress(json.dumps(data, ensure_ascii=False).encode("utf-8"))
    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=body,
        ContentType="application/json",
        ContentEncoding="gzip"
    )

    logger.info(f"bronze upload complete {s3_key}") 
    return f"s3://{bucket}/{s3_key}"

def write_to_silver_table(df: DataFrame, table_name: str) -> None:
    
    table_config = load_table_config("processed")
    partition_cols = table_config["partition_cols"]
    
    dataset = table_name.split(".")[-1]
    db = table_name.split(".")[0]
    path = f"s3a://{bucket}/processed/{dataset}/"
    
    df.sparkSession.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    
    (df.write
        .mode("overwrite")
        .partitionBy(*partition_cols)
        .format("parquet")
        .option("path", path)
        .option("compression", "snappy")
        .saveAsTable(table_name)
        )
    
    logger.info(f"silver upload complete to {table_name} at {path}")