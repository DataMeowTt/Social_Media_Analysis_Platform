from __future__ import annotations
from datetime import datetime
from src.utils.config_loader import load_config
from src.utils.aws_session import get_s3_client

config = load_config()
bucket = config["s3"]["bucket_name"]

def get_partition_prefix(partition_time: datetime, partition_cols: list[str]) -> str:

    partition_values = []
    for col in partition_cols:
        if col == "year":
            partition_values.append(f"year={partition_time.strftime('%Y')}")
        elif col == "month":
            partition_values.append(f"month={partition_time.strftime('%m')}")
        elif col == "day":
            partition_values.append(f"day={partition_time.strftime('%d')}")

    return "/".join(partition_values) + "/"
    
def get_s3_key(layer: str, dataset: str, filename: str, partition_time: datetime, partition_cols: list[str]) -> str:
    partition_prefix = get_partition_prefix(partition_time, partition_cols)
    
    return f"{layer}/{dataset}/{partition_prefix}{filename}"

def latest_value(prefix: str, key: str) -> str:
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")

    values = []
    for object in response.get("CommonPrefixes", []):
        if f"{key}=" in object["Prefix"]:
            values.append(object["Prefix"].split("/")[-2].split("=")[1])

    if not values:
        raise ValueError(f"No partition found for prefix {prefix} and key {key}")
    return sorted(values)[-1]

def get_latest_partition_prefix(dataset: str, layer: str) -> str:
    base = f"{layer}/{dataset}/"
    year = latest_value(base, "year")
    month = latest_value(f"{base}year={year}/", "month")
    day = latest_value(f"{base}year={year}/month={month}/", "day")

    return f"s3a://{bucket}/{base}year={year}/month={month}/day={day}/"