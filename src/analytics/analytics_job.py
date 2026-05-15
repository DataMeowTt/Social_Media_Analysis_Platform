from pyspark.sql import SparkSession

from src.utils.logger import get_logger
from src.storage.s3.reader import read_all_silver, read_fact_tweets_window, read_latest_silver
from src.storage.s3.uploader import write_to_S3
from src.analytics.transformations.enrich import enrich_analytics
from src.analytics.transformations.aggregate import (
    build_fact_tweets,
    build_dim_brands,
    build_dim_authors,
    build_agg_brand_daily,
    build_agg_author_perf,
)
from src.analytics.validate.quality_checks import (
    validate_analytics,
    validate_gold,
    validate_agg_author_perf,
)
from src.ml.ml_job import run_ml_pipeline

logger = get_logger(__name__)
LAYER = "analytics"


def gold_processing(spark: SparkSession) -> None:
    logger.info("Starting silver → gold pipeline")

    enriched_df = enrich_analytics(
        read_latest_silver(spark, dataset="tweets").dropDuplicates(["id"]) # avoiding accidents duplicate in silver layer by code 
    )
    enriched_df.cache()

    validate_analytics(enriched_df)
    logger.info("Pre-ML validation passed — running sentiment inference...")

    n_executors = 1
    df = run_ml_pipeline(enriched_df.coalesce(n_executors))
    
    df = df.repartition(spark.sparkContext.defaultParallelism)
    
    df.cache()
    df.count()
    enriched_df.unpersist()
    logger.info("Sentiment inference complete — building gold tables...")

    fact_tweets     = build_fact_tweets(df)
    dim_brands      = build_dim_brands(spark)
    dim_authors     = build_dim_authors(df).cache()
    agg_brand_daily = build_agg_brand_daily(df).cache()

    validate_gold(fact_tweets, dim_authors, agg_brand_daily)
    logger.info("Gold validation passed — writing tables...")

    write_to_S3(fact_tweets,     "analytics.fact_tweets",     LAYER, partition_cols=["date", "primary_brand"])
    write_to_S3(dim_brands,      "analytics.dim_brands",      LAYER, partition_cols=[])
    write_to_S3(dim_authors,     "analytics.dim_authors",     LAYER, partition_cols=[])
    write_to_S3(agg_brand_daily, "analytics.agg_brand_daily", LAYER, partition_cols=["brand_name"])

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
    write_to_S3(agg_author_perf, "analytics.agg_author_perf", LAYER, partition_cols=[])
    
    agg_author_perf.unpersist()
    df_30d.unpersist()

    logger.info("Gold pipeline complete")
