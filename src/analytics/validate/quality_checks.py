from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _check(df: DataFrame, condition, check_name: str) -> None:
    failed_rows = df.filter(condition).select(*df.columns[:4]).limit(5).collect()
    if failed_rows:
        count = df.filter(condition).count()
        logger.error(f"[{check_name}] {count} rows failed. Sample:")
        for row in failed_rows:
            logger.error(f"  {row.asDict()}")
        raise ValueError(f"[{check_name}] {count} rows failed — see logs for details")


# ── Pre-ML validation (on silver-enriched df) ─────────────────────────────────

def validate_analytics(df: DataFrame) -> None:
    result = df.agg(
        F.sum((F.col("engagement_score").isNull() | (F.col("engagement_score") < 0)).cast("int")).alias("bad_engagement"),
        F.sum(F.col("is_bot").isNull().cast("int")).alias("null_is_bot"),
        F.count("*").alias("total"),
        F.sum((F.col("primary_brand") == "Unknown").cast("int")).alias("no_brand"),
    ).collect()[0]

    if result["bad_engagement"] > 0:
        raise ValueError(f"[invalid_engagement_score] {result['bad_engagement']} rows failed")
    if result["null_is_bot"] > 0:
        raise ValueError(f"[null_is_bot] {result['null_is_bot']} rows failed")
    if result["total"] > 0 and result["no_brand"] / result["total"] > 0.5:
        logger.warning(
            f"[brand_coverage] {result['no_brand']}/{result['total']} ({result['no_brand']/result['total']:.1%}) tweets have no brand detected"
        )


# ── Gold table validation ─────────────────────────────────────────────────────

def validate_gold(
    fact_tweets:     DataFrame,
    dim_authors:     DataFrame,
    agg_brand_daily: DataFrame,
) -> None:
    ft = fact_tweets.agg(
        F.sum((F.col("tweet_id").isNull() | F.col("author_id").isNull() | F.col("created_at_ts").isNull()).cast("int")).alias("null_keys"),
        F.sum(F.col("sentiment_label").isNull().cast("int")).alias("null_sentiment"),
        F.count("*").alias("total"),
        F.countDistinct("tweet_id").alias("unique_tweets"),
    ).collect()[0]

    if ft["null_keys"] > 0:
        raise ValueError(f"[fact_tweets_null_keys] {ft['null_keys']} rows failed")
    if ft["null_sentiment"] > 0:
        raise ValueError(f"[fact_tweets_null_sentiment] {ft['null_sentiment']} rows failed")
    if ft["total"] != ft["unique_tweets"]:
        raise ValueError(f"[fact_tweets_duplicate_tweet_id] {ft['total'] - ft['unique_tweets']} duplicates found")

    _check(dim_authors, F.col("author_id").isNull(), "dim_authors_null_keys")

    abd = agg_brand_daily.agg(
        F.sum((F.col("date").isNull() | F.col("brand_name").isNull() | F.col("lang").isNull()).cast("int")).alias("null_keys"),
        F.sum((F.col("total_mentions").isNull() | (F.col("total_mentions") <= 0)).cast("int")).alias("invalid_mentions"),
        F.sum((F.col("share_of_voice").isNotNull() & ((F.col("share_of_voice") < 0) | (F.col("share_of_voice") > 1))).cast("int")).alias("invalid_sov"),
    ).collect()[0]

    if abd["null_keys"] > 0:
        raise ValueError(f"[agg_brand_daily_null_keys] {abd['null_keys']} rows failed")
    if abd["invalid_mentions"] > 0:
        raise ValueError(f"[agg_brand_daily_invalid_mentions] {abd['invalid_mentions']} rows failed")
    if abd["invalid_sov"] > 0:
        raise ValueError(f"[agg_brand_daily_invalid_sov] {abd['invalid_sov']} rows failed")


def validate_agg_author_perf(df: DataFrame) -> None:
    _check(df, F.col("author_id").isNull(), "agg_author_perf_null_keys")
