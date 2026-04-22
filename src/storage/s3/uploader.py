import boto3
import json
import gzip
from datetime import datetime, timezone

from src.storage.s3.partitioning import get_s3_key, get_partition_prefix
from src.utils.config_loader import load_config, load_table_config
from src.utils.logger import get_logger
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

    s3 = boto3.client("s3", region_name=config["aws"]["region"])
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

def upload_to_silver_s3(df: DataFrame, dataset: str) -> None:
    table_config = load_table_config("processed")
    
    created_at = df.select("created_at_ts").first()[0]
    partition_prefix = get_partition_prefix(created_at, table_config["partition_cols"])
    
    path = f"s3a://{bucket}/processed/{dataset}/{partition_prefix}"

    df.write.mode("append").option("compression", "snappy").parquet(path)
    logger.info(f"Silver upload complete: {path}")