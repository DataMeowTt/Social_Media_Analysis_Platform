from pyspark.sql import SparkSession

from src.utils.logger import get_logger
from src.storage.s3.reader import read_all_silver, read_latest_silver, read_fact_tweets_window
from src.storage.s3.uploader import write_to_s3
from src.twitter.analytics.transformations.enrich import enrich_analytics
from src.twitter.analytics.transformations.aggregate import (
    build_fact_tweets,
    build_dim_brands,
    build_dim_authors,
    build_agg_brand_daily,
    build_agg_author_perf,
)
from src.twitter.analytics.validate.quality_checks import (
    validate_analytics,
    validate_gold,
    validate_agg_author_perf,
)
from src.ml.inference.batch_inference import add_sentiment as run_ml_pipeline

logger = get_logger(__name__)
LAYER = "analytics"
TWEETS_PREFIX = "analytics/tweets"


def gold_processing(spark: SparkSession) -> None:
    logger.info("Starting silver → gold pipeline")

    # Latest silver only: ML runs on new data only
    df_latest = enrich_analytics(
        read_latest_silver(spark, dataset="tweets").dropDuplicates(["id"])
    )
    df_latest.cache()

    validate_analytics(df_latest)
    logger.info("Pre-ML validation passed — running sentiment inference...")

    n_executors = 1  # model is loaded per-executor; coalesce keeps inference on one process
    df = run_ml_pipeline(df_latest.coalesce(n_executors))

    df = df.repartition(spark.sparkContext.defaultParallelism)

    df.cache()
    df.count()
    df_latest.unpersist()
    logger.info("Sentiment inference complete — building gold tables...")

    fact_tweets     = build_fact_tweets(df)
    agg_brand_daily = build_agg_brand_daily(df).cache()

    # All silver: dim_authors needs full author history, no ML needed
    df_all  = enrich_analytics(
        read_all_silver(spark, dataset="tweets").dropDuplicates(["id"])
    )
    dim_authors = build_dim_authors(df_all).cache()
    dim_brands  = build_dim_brands(spark)

    validate_gold(fact_tweets, dim_authors, agg_brand_daily)
    logger.info("Gold validation passed — writing tables...")

    write_to_s3(fact_tweets,     "analytics.fact_tweets",     LAYER, partition_cols=["date", "primary_brand"], path_override=f"{TWEETS_PREFIX}/fact_tweets")
    write_to_s3(dim_brands,      "analytics.dim_brands",      LAYER, partition_cols=[],                        path_override=f"{TWEETS_PREFIX}/dim_brands")
    write_to_s3(dim_authors,     "analytics.dim_authors",     LAYER, partition_cols=[],                        path_override=f"{TWEETS_PREFIX}/dim_authors")
    write_to_s3(agg_brand_daily, "analytics.agg_brand_daily", LAYER, mode="append", partition_cols=["brand_name"], path_override=f"{TWEETS_PREFIX}/agg_brand_daily")

    df.unpersist()
    dim_authors.unpersist()
    agg_brand_daily.unpersist()

    # agg_author_perf reads 30-day window from fact_tweets
    logger.info("Building agg_author_perf from 30-day fact_tweets window...")
    df_30d = read_fact_tweets_window(spark, days=30)
    df_30d.cache()
    agg_author_perf = build_agg_author_perf(df_30d)
    agg_author_perf.cache()
    validate_agg_author_perf(agg_author_perf)
    write_to_s3(agg_author_perf, "analytics.agg_author_perf", LAYER, partition_cols=[], path_override=f"{TWEETS_PREFIX}/agg_author_perf")
    
    agg_author_perf.unpersist()
    df_30d.unpersist()

    logger.info("Gold pipeline complete")
