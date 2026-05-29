from __future__ import annotations

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from src.ml.inference.youtube_stance import predict_stance
from src.storage.s3.reader import read_latest_silver_facebook
from src.storage.s3.uploader import write_to_s3
from src.utils.config_loader import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)

_bucket = load_config()["s3"]["bucket_name"]

_STANCE_SCHEMA = StructType([
    StructField("post_id",           StringType(), False),
    StructField("post_text",         StringType(), True),
    StructField("comment_id",        StringType(), False),
    StructField("comment_text",      StringType(), True),
    StructField("comment_level",     IntegerType(), False),
    StructField("parent_comment_id", StringType(), True),
    StructField("stance",            StringType(), False),
    StructField("ingestion_date",    DateType(),   True),
    StructField("page_name",         StringType(), True),
])

_STANCE_MAP = {"agreement": "agree", "disagreement": "disagree", "neutral": "neutral"}


def _processed_partitions(spark: SparkSession):
    try:
        return (
            spark.read.parquet(f"s3a://{_bucket}/analytics/facebook/fact_comment_stance/")
            .select("ingestion_date", "page_name")
            .distinct()
        )
    except Exception:
        return None


def run_stance_analysis(spark: SparkSession) -> None:
    logger.info("[stance_job] reading dim_posts...")
    posts_df    = spark.read.parquet(f"s3a://{_bucket}/analytics/facebook/dim_posts/")
    comments_df = read_latest_silver_facebook(spark, "comments")

    processed = _processed_partitions(spark)
    if processed is not None:
        comments_df = comments_df.join(processed, on=["ingestion_date", "page_name"], how="left_anti")
        logger.info("[stance_job] incremental mode — skipping already-processed partitions")

    eligible_posts = posts_df.filter(
        F.col("sentiment_label").isNotNull() & (F.col("sentiment_label") != "neutral")
        & F.col("text").isNotNull() & (F.trim(F.col("text")) != "")
    )

    if eligible_posts.count() == 0:
        logger.info("[stance_job] no non-neutral posts — done")
        return

    eligible_pdf = eligible_posts.select(
        "post_id", "text", "ingestion_date", "page_name"
    ).toPandas()

    logger.info(f"[stance_job] {len(eligible_pdf)} non-neutral posts → fetching comments...")
    post_ids      = eligible_pdf["post_id"].tolist()
    post_text_map = dict(zip(eligible_pdf["post_id"], eligible_pdf["text"]))

    comments_pdf = (
        comments_df
        .filter(F.col("post_id").isin(post_ids))
        .select("comment_id", "post_id", "text", "is_reply", "parent_comment_id",
                "ingestion_date", "page_name")
        .toPandas()
    )

    if comments_pdf.empty:
        logger.info("[stance_job] no comments found — done")
        return

    comments_pdf["comment_text"] = comments_pdf["text"].apply(
        lambda t: " ".join(str(t).split()[:50]) if pd.notna(t) and t else ""
    )
    comments_pdf["post_text"] = comments_pdf["post_id"].map(post_text_map)

    before = len(comments_pdf)
    comments_pdf = comments_pdf[
        ~comments_pdf["text"].str.contains(r"\d+\s*tr\b", case=False, na=False, regex=True)
    ]
    logger.info(f"[stance_job] filtered {before - len(comments_pdf)} price comments, {len(comments_pdf)} remaining")

    if comments_pdf.empty:
        logger.info("[stance_job] no comments after filtering — done")
        return

    logger.info(f"[stance_job] running zero-shot stance on {len(comments_pdf)} comments...")
    stances = predict_stance(
        comments_pdf["comment_text"].tolist(),
        comments_pdf["post_text"].tolist(),
    )

    comments_pdf["stance"]        = [_STANCE_MAP.get(s, "neutral") for s in stances]
    comments_pdf["comment_level"] = comments_pdf["is_reply"].apply(lambda x: 2 if bool(x) else 1)

    result_pdf = comments_pdf[[
        "post_id", "post_text", "comment_id", "comment_text",
        "comment_level", "parent_comment_id", "stance", "ingestion_date", "page_name",
    ]]

    logger.info(f"[stance_job] writing {len(result_pdf)} rows...")
    result_df = spark.createDataFrame(result_pdf, schema=_STANCE_SCHEMA)
    write_to_s3(
        result_df,
        table_name="analytics.fb_fact_comment_stance",
        layer="analytics",
        mode="append",
        partition_cols=["ingestion_date", "page_name"],
        path_override="analytics/facebook/fact_comment_stance",
    )
    logger.info("[stance_job] done")
