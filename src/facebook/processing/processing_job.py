from __future__ import annotations

from pyspark.sql import SparkSession

from src.facebook.processing.transformations.normalize import flatten_comments, flatten_posts
from src.storage.s3.reader import read_all_bronze, read_latest_bronze
from src.storage.s3.uploader import write_to_s3
from src.utils.s3_deduplicator import delete_full
from src.utils.logger import get_logger

logger = get_logger(__name__)

_DATASET = "facebook"
_PARTITION_COLS = ["ingestion_date"]


def _write_silver(posts_df, comments_df, mode: str = "overwrite") -> None:
    write_to_s3(
        posts_df,
        table_name="processed.facebook_posts",
        layer="processed",
        mode=mode,
        partition_cols=_PARTITION_COLS,
        path_override="processed/facebook/posts",
    )
    write_to_s3(
        comments_df,
        table_name="processed.facebook_comments",
        layer="processed",
        mode=mode,
        partition_cols=_PARTITION_COLS,
        path_override="processed/facebook/comments",
    )


def historical_processing(spark: SparkSession) -> None:
    logger.info("[facebook] Starting bronze → silver (historical)")
    delete_full("processed/facebook/")

    df = read_all_bronze(spark, dataset=_DATASET)
    logger.info("[facebook] Bronze read complete — flattening...")

    posts_df    = flatten_posts(df)
    comments_df = flatten_comments(df)

    posts_df.cache()
    comments_df.cache()
    try:
        logger.info(f"[facebook] posts={posts_df.count()}  comments={comments_df.count()}")
        _write_silver(posts_df, comments_df, mode="overwrite")
        logger.info("[facebook] Silver complete (historical)")
    finally:
        posts_df.unpersist()
        comments_df.unpersist()


def incremental_processing(spark: SparkSession) -> None:
    logger.info("[facebook] Starting bronze → silver (incremental)")

    df = read_latest_bronze(spark, dataset=_DATASET)
    logger.info("[facebook] Bronze read complete — flattening...")

    posts_df    = flatten_posts(df)
    comments_df = flatten_comments(df)

    posts_df.cache()
    comments_df.cache()
    try:
        logger.info(f"[facebook] posts={posts_df.count()}  comments={comments_df.count()}")
        _write_silver(posts_df, comments_df, mode="append")
        logger.info("[facebook] Silver complete (incremental)")
    finally:
        posts_df.unpersist()
        comments_df.unpersist()
