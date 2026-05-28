import io
import os

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

_BUCKET = os.getenv("S3_BUCKET", "social-analysis-prod-bucket")
_REGION = os.getenv("AWS_REGION", "ap-southeast-1")


def _s3():
    return boto3.client(
        "s3",
        region_name=_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def _read_parquet(prefix: str) -> pd.DataFrame:
    s3 = _s3()
    paginator = s3.get_paginator("list_objects_v2")
    tables = []
    for page in paginator.paginate(Bucket=_BUCKET, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".parquet"):
                continue
            body = s3.get_object(Bucket=_BUCKET, Key=obj["Key"])["Body"].read()
            tables.append(pq.read_table(io.BytesIO(body)))
    if not tables:
        return pd.DataFrame()
    return pa.concat_tables(tables).to_pandas()


def controversial_posts() -> list[dict]:
    posts = _read_parquet("analytics/facebook/dim_posts")
    stance = _read_parquet("analytics/facebook/fact_comment_stance")
    if posts.empty or stance.empty:
        return []

    agg = (
        stance.groupby("post_id")
        .agg(
            agree_count=("stance", lambda x: (x == "agree").sum()),
            disagree_count=("stance", lambda x: (x == "disagree").sum()),
            total_comments=("comment_id", "count"),
        )
        .reset_index()
    )

    post_cols = ["post_id", "text_preview", "sentiment_label", "page_name"]
    if "permalink" in posts.columns:
        post_cols.append("permalink")

    merged = agg.merge(posts[post_cols], on="post_id", how="inner")

    result = (
        merged.sort_values("total_comments", ascending=False)
        .head(2)
        .drop(columns=["post_id"])
    )
    return result.where(pd.notnull(result), None).to_dict(orient="records")
