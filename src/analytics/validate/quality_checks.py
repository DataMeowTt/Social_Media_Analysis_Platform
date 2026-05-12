from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _check(df: DataFrame, condition, check_name: str) -> None:
    count = df.filter(condition).count()
    if count > 0:
        logger.error(f"[{check_name}] {count} rows failed. Sample:")
        for row in df.filter(condition).select(*df.columns[:4]).limit(5).collect():
            logger.error(f"  {row.asDict()}")
        raise ValueError(f"[{check_name}] {count} rows failed — see logs for details")


# ── Pre-ML validation (on silver-enriched df) ─────────────────────────────────

def assert_engagement_score_valid(df: DataFrame) -> None:
    _check(df, F.col("engagement_score").isNull() | (F.col("engagement_score") < 0), "invalid_engagement_score")


def assert_is_viral_not_null(df: DataFrame) -> None:
    _check(df, F.col("is_viral").isNull(), "null_is_viral")


def assert_author_influence_non_negative(df: DataFrame) -> None:
    _check(
        df,
        F.col("author_influence_ratio").isNotNull() & (F.col("author_influence_ratio") < 0),
        "negative_author_influence_ratio",
    )


def assert_is_bot_not_null(df: DataFrame) -> None:
    _check(df, F.col("is_bot").isNull(), "null_is_bot")


def assert_brand_coverage(df: DataFrame) -> None:
    """Warn (not raise) if more than 50 % of tweets have no brand detected."""
    total    = df.count()
    no_brand = df.filter(F.col("primary_brand") == "Unknown").count()
    ratio    = no_brand / total if total > 0 else 0
    if ratio > 0.5:
        logger.warning(
            f"[brand_coverage] {no_brand}/{total} ({ratio:.1%}) tweets have no brand detected"
        )


def validate_analytics(df: DataFrame) -> None:
    assert_engagement_score_valid(df)
    assert_is_viral_not_null(df)
    assert_author_influence_non_negative(df)
    assert_is_bot_not_null(df)
    assert_brand_coverage(df)


# ── Gold table validation ─────────────────────────────────────────────────────

def validate_gold(
    fact_tweets:     DataFrame,
    dim_brands:      DataFrame,
    dim_authors:     DataFrame,
    agg_brand_daily: DataFrame,
    agg_author_perf: DataFrame,
) -> None:
    _check(
        fact_tweets,
        F.col("tweet_id").isNull() | F.col("author_id").isNull() | F.col("created_at_ts").isNull(),
        "fact_tweets_null_keys",
    )
    _check(fact_tweets, F.col("sentiment_label").isNull(), "fact_tweets_null_sentiment")

    brand_count = dim_brands.count()
    if brand_count != 10:
        raise ValueError(f"[dim_brands] Expected 10 rows, got {brand_count}")
    _check(dim_brands, F.col("brand_id").isNull() | F.col("brand_name").isNull(), "dim_brands_null_keys")

    _check(dim_authors, F.col("author_id").isNull(), "dim_authors_null_keys")

    _check(
        agg_brand_daily,
        F.col("date").isNull() | F.col("brand_name").isNull() | F.col("lang").isNull(),
        "agg_brand_daily_null_keys",
    )
    _check(
        agg_brand_daily,
        F.col("total_mentions").isNull() | (F.col("total_mentions") <= 0),
        "agg_brand_daily_invalid_mentions",
    )
    _check(
        agg_brand_daily,
        F.col("share_of_voice").isNotNull()
        & ((F.col("share_of_voice") < 0) | (F.col("share_of_voice") > 1)),
        "agg_brand_daily_invalid_sov",
    )

    _check(agg_author_perf, F.col("author_id").isNull(), "agg_author_perf_null_keys")
