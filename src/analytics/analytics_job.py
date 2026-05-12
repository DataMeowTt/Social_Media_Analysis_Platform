from pyspark.sql import SparkSession

from src.utils.logger import get_logger
from src.storage.s3.reader import read_silver
from src.storage.s3.uploader import write_to_S3
from src.analytics.transformations.enrich import enrich_analytics
from src.analytics.transformations.aggregate import (
    build_fact_tweets,
    build_dim_brands,
    build_dim_authors,
    build_agg_brand_daily,
    build_agg_author_perf,
)
from src.analytics.validate.quality_checks import validate_analytics, validate_gold
from src.ml.ml_job import run_ml_pipeline

logger = get_logger(__name__)
LAYER = "analytics"


def gold_processing(spark: SparkSession) -> None:
    logger.info("Starting silver → gold pipeline")

    df = read_silver(spark, dataset="tweets")

    df = enrich_analytics(df)
    df.cache()

    validate_analytics(df)
    logger.info("Pre-ML validation passed — running sentiment inference...")

    df = run_ml_pipeline(df)
    logger.info("Sentiment inference complete — building gold tables...")

    fact_tweets     = build_fact_tweets(df)
    dim_brands      = build_dim_brands(spark)
    dim_authors     = build_dim_authors(df)
    agg_brand_daily = build_agg_brand_daily(df)
    agg_author_perf = build_agg_author_perf(df)

    validate_gold(fact_tweets, dim_brands, dim_authors, agg_brand_daily, agg_author_perf)
    logger.info("Gold validation passed — writing tables...")

    write_to_S3(fact_tweets,     "analytics.fact_tweets",     LAYER, partition_cols=["date", "primary_brand"])
    write_to_S3(dim_brands,      "analytics.dim_brands",      LAYER, partition_cols=[])
    write_to_S3(dim_authors,     "analytics.dim_authors",     LAYER, partition_cols=[])
    write_to_S3(agg_brand_daily, "analytics.agg_brand_daily", LAYER, partition_cols=["brand_name", "date"])
    write_to_S3(agg_author_perf, "analytics.agg_author_perf", LAYER, partition_cols=[])

    df.unpersist()
    logger.info("Gold pipeline complete")
