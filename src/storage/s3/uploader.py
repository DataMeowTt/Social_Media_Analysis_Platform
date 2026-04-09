import boto3
import json
import gzip
import io
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from src.storage.s3.partitioning import get_s3_key
from src.utils.config_loader import load_config, load_table_config
from src.utils.logger import get_logger

logger = get_logger(__name__)

def upload_to_s3(data: list[dict], dataset: str, layer: str, ingestion_time: datetime = None) -> str:
    config = load_config()
    table_config = load_table_config(layer)

    bucket = config["s3"]["bucket_name"]
    fmt = table_config["format"]
    partition_cols = table_config["partition_cols"]

    if ingestion_time is None:
        ingestion_time = datetime.now(timezone.utc)

    if fmt == "json":
        filename = f"{dataset}_{ingestion_time.strftime('%Y%m%d_%H%M%S')}.json.gz"
    elif fmt == "parquet":
        filename = f"{dataset}_{ingestion_time.strftime('%Y%m%d_%H%M%S')}.parquet"
    else:
        logger.error(f"Unsupported format: {fmt}")
        raise ValueError(f"Unsupported format: {fmt}")

    s3_key = get_s3_key(
        layer=layer,
        dataset=dataset,
        filename=filename,
        ingestion_time=ingestion_time,
        partition_cols=partition_cols
    )

    s3 = boto3.client("s3", region_name=config["aws"]["region"])
    
    if fmt == "json":
        body = gzip.compress(json.dumps(data, ensure_ascii=False).encode("utf-8"))
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=body,
            ContentType="application/json",
            ContentEncoding="gzip"
        )
    elif fmt == "parquet":
        table = pa.Table.from_pylist(data)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        buffer.seek(0)
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=buffer.read(),
            ContentType="application/octet-stream"
        )

    logger.info(f"Data uploaded to S3 at {s3_key}") 

    return f"s3://{bucket}/{s3_key}"

