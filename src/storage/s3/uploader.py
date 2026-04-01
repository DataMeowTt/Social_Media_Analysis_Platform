import boto3
import json
from datetime import datetime, timezone
from src.storage.s3.partitioning import get_s3_key
from src.utils.config_loader import load_config, load_table_config

def upload_to_s3(data: list[dict], dataset: str, layer: str, ingestion_time: datetime = None) -> str:
    config = load_config()
    table_config = load_table_config(layer)

    bucket = config["s3"]["bucket_name"]
    region = config["aws"]["region"]
    fmt = table_config["format"]
    partition_cols = table_config["partition_cols"]

    if ingestion_time is None:
        ingestion_time = datetime.now(timezone.utc)

    s3_key = get_s3_key(
        layer=layer,
        dataset=dataset,
        filename=f"{dataset}_{ingestion_time.strftime('%Y%m%d_%H%M%S')}.{fmt}",
        ingestion_time=ingestion_time,
        partition_cols=partition_cols
    )

    s3 = boto3.client("s3", region_name=region)
    
    if fmt == "json":
        s3.put_object(
           Bucket=bucket,
           Key=s3_key,
           Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
           ContentType="application/json"
        )
    elif fmt == "parquet":
        # TODO Implement parquet upload later.
        pass

    return f"s3://{bucket}/{s3_key}"

