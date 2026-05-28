from __future__ import annotations

from datetime import date as _date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.facebook.analytics.transformations.aggregate import (
    build_agg_page_daily,
    build_dim_posts,
    build_fact_comments,
)
from src.facebook.analytics.transformations.enrich import enrich_comments
from src.facebook.analytics.validate.quality_checks import validate_gold, validate_silver_comments
from src.ml.inference.gemini_sentiment import add_sentiment as run_ml_pipeline
from src.storage.s3.reader import read_all_silver, read_latest_silver_facebook
from src.storage.s3.uploader import write_to_S3
from src.utils.logger import get_logger

logger = get_logger(__name__)


def gold_processing(spark: SparkSession) -> None:
    logger.info("[facebook] Starting silver → gold (ML path)")

    posts_df    = read_all_silver(spark, dataset="facebook/posts")
    comments_df = read_all_silver(spark, dataset="facebook/comments")

    validate_silver_comments(comments_df)

    logger.info("[facebook] Running enrichment...")
    enriched = enrich_comments(comments_df)

    logger.info("[facebook] Running ML sentiment inference...")
    n_executors = 1
    enriched_with_sentiment = run_ml_pipeline(enriched.coalesce(n_executors))
    enriched_with_sentiment = enriched_with_sentiment.repartition(
        spark.sparkContext.defaultParallelism
    )
    enriched_with_sentiment.cache()
    enriched_with_sentiment.count()
    logger.info("[facebook] Sentiment inference complete — building gold tables...")

    fact_comments  = build_fact_comments(enriched_with_sentiment)
    agg_page_daily = build_agg_page_daily(enriched_with_sentiment).cache()

    validate_gold(fact_comments, agg_page_daily)
    logger.info("[facebook] Gold validation passed — writing tables...")

    write_to_S3(
        fact_comments,
        table_name="analytics.fb_fact_comments",
        layer="analytics",
        mode="overwrite",
        partition_cols=["date", "page_name"],
        path_override="analytics/facebook/fact_comments",
    )
    write_to_S3(
        agg_page_daily,
        table_name="analytics.fb_agg_page_daily",
        layer="analytics",
        mode="overwrite",
        partition_cols=["page_name"],
        path_override="analytics/facebook/agg_page_daily",
    )

    posts_with_text = posts_df.filter(
        F.col("text").isNotNull() & (F.trim(F.col("text")) != "")
    )
    posts_no_text = posts_df.filter(
        F.col("text").isNull() | (F.trim(F.col("text")) == "")
    ).withColumn("sentiment_label", F.lit(None).cast("string")) \
     .withColumn("sentiment_score",  F.lit(None).cast("float"))

    posts_enriched = run_ml_pipeline(posts_with_text.coalesce(1))
    all_posts = posts_enriched.unionByName(posts_no_text)
    dim_posts = build_dim_posts(all_posts)
    write_to_S3(
        dim_posts,
        table_name="analytics.fb_dim_posts",
        layer="analytics",
        mode="overwrite",
        partition_cols=[],
        path_override="analytics/facebook/dim_posts",
    )

    enriched_with_sentiment.unpersist()
    agg_page_daily.unpersist()
    logger.info("[facebook] Gold pipeline (ML path) complete")


def incremental_gold_processing(spark: SparkSession) -> None:
    logger.info(f"[facebook] Starting silver → gold (incremental, date={_date.today()})")

    comments_df = read_latest_silver_facebook(spark, "comments")

    validate_silver_comments(comments_df)

    logger.info("[facebook] Running enrichment...")
    enriched = enrich_comments(comments_df)

    logger.info("[facebook] Running ML sentiment inference on today's comments...")
    n_executors = 1
    enriched_with_sentiment = run_ml_pipeline(enriched.coalesce(n_executors))
    enriched_with_sentiment = enriched_with_sentiment.repartition(
        spark.sparkContext.defaultParallelism
    )
    enriched_with_sentiment.cache()
    enriched_with_sentiment.count()
    logger.info("[facebook] Sentiment inference complete — building gold tables...")

    fact_comments  = build_fact_comments(enriched_with_sentiment)
    agg_page_daily = build_agg_page_daily(enriched_with_sentiment).cache()

    validate_gold(fact_comments, agg_page_daily)
    logger.info("[facebook] Gold validation passed — writing tables...")

    write_to_S3(
        fact_comments,
        table_name="analytics.fb_fact_comments",
        layer="analytics",
        mode="append",
        partition_cols=["date", "page_name"],
        path_override="analytics/facebook/fact_comments",
    )
    write_to_S3(
        agg_page_daily,
        table_name="analytics.fb_agg_page_daily",
        layer="analytics",
        mode="append",
        partition_cols=["page_name"],
        path_override="analytics/facebook/agg_page_daily",
    )

    enriched_with_sentiment.unpersist()
    agg_page_daily.unpersist()

    # dim_posts là dimension table toàn phần — rebuild từ tất cả silver posts
    all_posts_df = read_all_silver(spark, dataset="facebook/posts")
    posts_with_text = all_posts_df.filter(
        F.col("text").isNotNull() & (F.trim(F.col("text")) != "")
    )
    posts_no_text = all_posts_df.filter(
        F.col("text").isNull() | (F.trim(F.col("text")) == "")
    ).withColumn("sentiment_label", F.lit(None).cast("string")) \
     .withColumn("sentiment_score",  F.lit(None).cast("float"))

    posts_enriched = run_ml_pipeline(posts_with_text.coalesce(1))
    all_posts = posts_enriched.unionByName(posts_no_text)
    dim_posts = build_dim_posts(all_posts)
    write_to_S3(
        dim_posts,
        table_name="analytics.fb_dim_posts",
        layer="analytics",
        mode="overwrite",
        partition_cols=[],
        path_override="analytics/facebook/dim_posts",
    )

    logger.info("[facebook] Incremental gold pipeline complete")
