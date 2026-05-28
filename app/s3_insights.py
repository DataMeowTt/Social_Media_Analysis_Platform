import io
import os

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

_BUCKET = os.getenv("S3_BUCKET", "social-analysis-prod-bucket")
_PREFIX = "analytics/youtube/thread_insights"
_REGION = os.getenv("AWS_REGION", "ap-southeast-1")

_FRONTEND_COLS = {
    "thread_id", "video_id", "entity", "aspect", "sentiment",
    "intensity", "mention_count", "evidence", "brand_comment_count", "analyzed_at",
}


def _s3():
    return boto3.client(
        "s3",
        region_name=_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def read_thread_insights() -> list[dict]:
    s3 = _s3()
    paginator = s3.get_paginator("list_objects_v2")

    tables = []
    for page in paginator.paginate(Bucket=_BUCKET, Prefix=_PREFIX + "/"):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".parquet"):
                continue
            body = s3.get_object(Bucket=_BUCKET, Key=obj["Key"])["Body"].read()
            tables.append(pq.read_table(io.BytesIO(body)))

    if not tables:
        return []

    df = pa.concat_tables(tables).to_pandas()
    df = df[[c for c in df.columns if c in _FRONTEND_COLS]]

    # Timestamps → ISO strings so FastAPI can serialize them
    ts_cols = df.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]"]).columns
    for col in ts_cols:
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    return df.to_dict(orient="records")
