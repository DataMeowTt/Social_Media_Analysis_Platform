from __future__ import annotations

from typing import Set

import gzip
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.utils.session import create_spark_session, get_s3_client

logger = get_logger(__name__)
_config = load_config()
_BUCKET = _config["s3"]["bucket_name"]

_REPLY_THRESHOLD = 50


def _sentiment_path() -> str:
    return f"s3a://{_BUCKET}/analytics/youtube/comments/"


def _stance_path() -> str:
    return f"s3a://{_BUCKET}/analytics/youtube/stance/"


def _insights_path() -> str:
    return f"s3a://{_BUCKET}/analytics/youtube/thread_insights/"


def _already_analyzed(spark: SparkSession, video_id: str) -> Set[str]:
    try:
        existing = spark.read.parquet(_insights_path()).filter(F.col("video_id") == video_id)
        return {row.thread_id for row in existing.select("thread_id").distinct().collect()}
    except Exception:
        return set()


def build_conversations(spark: SparkSession, video_id: str, force: bool = False) -> list[dict]:
    sentiment_df = spark.read.parquet(_sentiment_path()).filter(F.col("video_id") == video_id)
    stance_df = spark.read.parquet(_stance_path()).filter(F.col("video_id") == video_id)

    candidates = (
        stance_df
        .groupBy("parent_id")
        .agg(F.count("*").alias("reply_count"))
        .filter(F.col("reply_count") > _REPLY_THRESHOLD)
    )

    if not force:
        analyzed = _already_analyzed(spark, video_id)
        if analyzed:
            candidates = candidates.filter(~F.col("parent_id").isin(analyzed))
            logger.info(f"Skipping {len(analyzed)} already-analyzed threads")

    thread_rows = candidates.collect()
    if not thread_rows:
        logger.info("No candidate threads found")
        return []

    thread_ids = [row.parent_id for row in thread_rows]
    logger.info(f"Found {len(thread_ids)} candidate threads")

    roots = (
        sentiment_df
        .filter(F.col("is_root"))
        .filter(F.col("comment_id").isin(thread_ids))
        .withColumn("thread_id", F.col("comment_id"))
    )

    replies = (
        sentiment_df
        .filter(~F.col("is_root"))
        .join(
            stance_df.select("comment_id", "parent_id"),
            on="comment_id",
            how="left",
        )
        .filter(F.col("parent_id").isin(thread_ids))
        .withColumnRenamed("parent_id", "thread_id")
    )

    cols = ["comment_id", "thread_id", "text", "brand_mentioned",
            "sentiment", "author_name", "is_root"]
    all_pdf = roots.select(*cols).union(replies.select(*cols)).toPandas()

    conversations = []
    for thread_id, group in all_pdf.groupby("thread_id"):
        comments = group.to_dict("records")
        comments.sort(key=lambda c: not c["is_root"])  # root first
        conversations.append({
            "thread_id": thread_id,
            "video_id": video_id,
            "brand_comment_count": len(comments),
            "comments": comments,
        })

    logger.info(f"Built {len(conversations)} conversations")
    return conversations


def run_conversation_builder(youtube_id: str, force: bool = False) -> None:
    spark = create_spark_session("YouTube-Conversation-Builder")
    try:
        conversations = build_conversations(spark, youtube_id, force=force)
    finally:
        spark.stop()

    s3_key = f"analytics/youtube/conversations/{youtube_id}/threads.json.gz"
    body = gzip.compress(json.dumps(conversations, ensure_ascii=False, default=str).encode())
    get_s3_client().put_object(Bucket=_BUCKET, Key=s3_key, Body=body)
    logger.info(f"Wrote {len(conversations)} conversations → s3://{_BUCKET}/{s3_key}")
