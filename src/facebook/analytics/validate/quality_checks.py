from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import get_logger

logger = get_logger(__name__)


def validate_silver_posts(df: DataFrame) -> None:
    null_ids = df.filter(F.col("post_id").isNull()).count()
    if null_ids > 0:
        raise ValueError(f"[silver_posts] {null_ids} rows with null post_id")


def validate_silver_comments(df: DataFrame) -> None:
    result = df.agg(
        F.sum(F.col("comment_id").isNull().cast("int")).alias("null_comment_id"),
        F.sum(F.col("post_id").isNull().cast("int")).alias("null_post_id"),
        F.count("*").alias("total"),
    ).collect()[0]

    if result["null_comment_id"] > 0:
        raise ValueError(f"[silver_comments] {result['null_comment_id']} rows with null comment_id")
    if result["null_post_id"] > 0:
        raise ValueError(f"[silver_comments] {result['null_post_id']} rows with null post_id")
    logger.info(f"[silver_comments] validation passed — {result['total']} rows")


def validate_gold(fact_comments: DataFrame, agg_page_daily: DataFrame) -> None:
    fc = fact_comments.agg(
        F.sum(F.col("comment_id").isNull().cast("int")).alias("null_keys"),
        F.sum(F.col("sentiment_label").isNull().cast("int")).alias("null_sentiment"),
        F.count("*").alias("total"),
        F.countDistinct("comment_id").alias("unique_ids"),
    ).collect()[0]

    if fc["null_keys"] > 0:
        raise ValueError(f"[fact_comments] {fc['null_keys']} rows with null comment_id")
    if fc["null_sentiment"] > 0:
        raise ValueError(f"[fact_comments] {fc['null_sentiment']} rows with null sentiment_label")
    if fc["total"] != fc["unique_ids"]:
        raise ValueError(f"[fact_comments] {fc['total'] - fc['unique_ids']} duplicate comment_ids")

    apd = agg_page_daily.agg(
        F.sum((F.col("date").isNull() | F.col("page_name").isNull()).cast("int")).alias("null_keys"),
        F.sum(
            (F.col("positive_ratio").isNotNull() & (F.col("positive_ratio") > 1)).cast("int")
        ).alias("invalid_ratio"),
    ).collect()[0]

    if apd["null_keys"] > 0:
        raise ValueError(f"[agg_page_daily] {apd['null_keys']} rows with null keys")
    if apd["invalid_ratio"] > 0:
        logger.warning(f"[agg_page_daily] {apd['invalid_ratio']} rows with positive_ratio > 1")

    logger.info(f"[gold] validation passed — fact_comments: {fc['total']} rows")
