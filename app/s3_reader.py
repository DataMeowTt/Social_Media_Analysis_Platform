"""Shared S3 Parquet reader used by dashboard insight modules."""
import io
import os

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

_BUCKET = os.getenv("S3_BUCKET") or os.getenv("S3_BUCKET_NAME", "social-analysis-prod-bucket")
_REGION = os.getenv("AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "ap-southeast-1"))


def _s3():
    return boto3.client(
        "s3",
        region_name=_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def read_parquet_from_s3(prefix: str) -> "pa.Table | None":
    """Read all Parquet files under *prefix* and return a concatenated PyArrow Table.

    Returns None if no files are found.
    """
    s3 = _s3()
    paginator = s3.get_paginator("list_objects_v2")
    tables = []
    for page in paginator.paginate(Bucket=_BUCKET, Prefix=prefix.rstrip("/") + "/"):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".parquet"):
                continue
            body = s3.get_object(Bucket=_BUCKET, Key=obj["Key"])["Body"].read()
            tables.append(pq.read_table(io.BytesIO(body)))
    return pa.concat_tables(tables) if tables else None
