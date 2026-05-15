import pytest
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    IntegerType, BooleanType, ArrayType, TimestampType, DoubleType,
)

from src.twitter.analytics.transformations.enrich import (
    add_engagement_score, add_is_viral, add_author_influence, add_brands, add_is_bot,
    enrich_analytics,
)
from src.twitter.analytics.transformations.aggregate import (
    build_fact_tweets, build_dim_brands, build_dim_authors,
    build_agg_brand_daily, build_agg_author_perf,
)
from src.twitter.analytics.validate.quality_checks import validate_analytics, validate_gold

# ── Silver schema (output of processing pipeline) ─────────────────────────────

SILVER_SCHEMA = StructType([
    StructField("id",                    StringType(),            True),
    StructField("type",                  StringType(),            True),
    StructField("text",                  StringType(),            True),
    StructField("lang",                  StringType(),            True),
    StructField("retweetCount",          LongType(),              True),
    StructField("replyCount",            LongType(),              True),
    StructField("likeCount",             LongType(),              True),
    StructField("quoteCount",            LongType(),              True),
    StructField("author_id",             StringType(),            True),
    StructField("author_username",       StringType(),            True),
    StructField("author_followers",      IntegerType(),           True),
    StructField("author_following",      IntegerType(),           True),
    StructField("author_statuses_count", IntegerType(),           True),
    StructField("author_is_blue_verified", BooleanType(),         True),
    StructField("created_at_ts",         TimestampType(),         True),
    StructField("hashtags",              ArrayType(StringType()), True),
    StructField("inReplyToId",           StringType(),            True),
    StructField("mentioned_user_ids",    ArrayType(StringType()), True),
])

_TS = datetime(2026, 5, 9, 8, 14, 27)


def _silver_row(**overrides) -> tuple:
    defaults = {
        "id": "1", "type": "tweet", "text": "hello world", "lang": "en",
        "retweetCount": 10, "replyCount": 2, "likeCount": 5, "quoteCount": 1,
        "author_id": "auth1", "author_username": "user1",
        "author_followers": 1000, "author_following": 500,
        "author_statuses_count": 1000,
        "author_is_blue_verified": False,
        "created_at_ts": _TS,
        "hashtags": None, "inReplyToId": None, "mentioned_user_ids": None,
    }
    d = {**defaults, **overrides}
    return tuple(d[f.name] for f in SILVER_SCHEMA.fields)


def _silver(spark, **overrides):
    return spark.createDataFrame([_silver_row(**overrides)], SILVER_SCHEMA)


def _enriched(spark, **overrides):
    """Silver df passed through enrich_analytics."""
    return enrich_analytics(_silver(spark, **overrides))


def _gold_ready(spark, **overrides):
    """Enriched df with sentiment columns added (simulates post-ML state)."""
    return (
        _enriched(spark, **overrides)
        .withColumn("sentiment_label", F.lit("positive"))
        .withColumn("sentiment_score", F.lit(0.9).cast("float"))
    )


# ── add_engagement_score ──────────────────────────────────────────────────────

def test_engagement_score_formula(spark):
    # score = 2*rt + like + 1.5*reply + 1.5*quote
    # = 2*10 + 5 + 1.5*2 + 1.5*1 = 20 + 5 + 3 + 1.5 = 29.5
    df = add_engagement_score(_silver(spark, retweetCount=10, likeCount=5, replyCount=2, quoteCount=1))
    assert df.collect()[0]["engagement_score"] == 29.5


def test_engagement_score_zero_for_no_activity(spark):
    df = add_engagement_score(_silver(spark, retweetCount=0, likeCount=0, replyCount=0, quoteCount=0))
    assert df.collect()[0]["engagement_score"] == 0.0


# ── add_is_viral ──────────────────────────────────────────────────────────────

def test_is_viral_above_threshold(spark):
    df = _silver(spark, retweetCount=500, likeCount=0, replyCount=0, quoteCount=0)
    df = add_engagement_score(df)
    df = add_is_viral(df, threshold=1000)
    assert df.collect()[0]["is_viral"] is True  # 500*2 = 1000 >= 1000


def test_is_viral_below_threshold(spark):
    df = _silver(spark, retweetCount=0, likeCount=1, replyCount=0, quoteCount=0)
    df = add_engagement_score(df)
    df = add_is_viral(df, threshold=1000)
    assert df.collect()[0]["is_viral"] is False


# ── add_author_influence ──────────────────────────────────────────────────────

def test_author_influence_ratio(spark):
    df = add_author_influence(_silver(spark, author_followers=1000, author_following=500))
    ratio = df.collect()[0]["author_influence_ratio"]
    assert ratio == pytest.approx(2.0)


def test_author_influence_null_when_following_zero(spark):
    df = add_author_influence(_silver(spark, author_followers=1000, author_following=0))
    assert df.collect()[0]["author_influence_ratio"] is None


# ── add_brands ────────────────────────────────────────────────────────────────

def test_brands_detected_in_text(spark):
    df = add_brands(_silver(spark, text="Tesla is great"))
    row = df.collect()[0]
    assert "Tesla" in row["all_brands"]
    assert row["primary_brand"] == "Tesla"


def test_brands_detected_in_hashtag(spark):
    df = add_brands(_silver(spark, text="no brand", hashtags=["tesla", "ev"]))
    row = df.collect()[0]
    assert "Tesla" in row["all_brands"]


def test_multiple_brands_detected(spark):
    df = add_brands(_silver(spark, text="Tesla vs BMW comparison"))
    brands = df.collect()[0]["all_brands"]
    assert "Tesla" in brands
    assert "BMW" in brands


def test_primary_brand_unknown_when_no_match(spark):
    df = add_brands(_silver(spark, text="random tweet no brand", hashtags=None))
    assert df.collect()[0]["primary_brand"] == "Unknown"


# ── add_is_bot ────────────────────────────────────────────────────────────────

def test_is_bot_true_for_high_activity_low_followers(spark):
    df = add_is_bot(_silver(spark, author_statuses_count=60_000, author_followers=100))
    assert df.collect()[0]["is_bot"] is True


def test_is_bot_false_for_normal_account(spark):
    df = add_is_bot(_silver(spark, author_statuses_count=1_000, author_followers=5_000))
    assert df.collect()[0]["is_bot"] is False


# ── build_dim_brands ──────────────────────────────────────────────────────────

def test_dim_brands_has_10_rows(spark):
    df = build_dim_brands(spark)
    assert df.count() == 10


def test_dim_brands_has_required_columns(spark):
    cols = build_dim_brands(spark).columns
    for c in ("brand_id", "brand_name", "brand_country", "brand_segment"):
        assert c in cols


def test_dim_brands_vinfast_present(spark):
    brands = [r["brand_name"] for r in build_dim_brands(spark).collect()]
    assert "VinFast" in brands


# ── build_fact_tweets ─────────────────────────────────────────────────────────

def test_fact_tweets_has_required_columns(spark):
    df = build_fact_tweets(_gold_ready(spark))
    for col in ("tweet_id", "author_id", "primary_brand", "created_at_ts",
                "sentiment_label", "sentiment_score", "engagement_score", "is_viral"):
        assert col in df.columns


def test_fact_tweets_row_count_preserved(spark):
    silver = spark.createDataFrame(
        [_silver_row(id="1"), _silver_row(id="2")], SILVER_SCHEMA
    )
    gold = enrich_analytics(silver) \
        .withColumn("sentiment_label", F.lit("neutral")) \
        .withColumn("sentiment_score", F.lit(0.5).cast("float"))
    assert build_fact_tweets(gold).count() == 2


# ── build_agg_brand_daily ─────────────────────────────────────────────────────

def _multi_brand_gold(spark):
    rows = [
        _silver_row(id="1", text="Tesla news", lang="en"),
        _silver_row(id="2", text="Tesla update", lang="en"),
        _silver_row(id="3", text="BMW story", lang="en"),
    ]
    silver = spark.createDataFrame(rows, SILVER_SCHEMA)
    return (
        enrich_analytics(silver)
        .withColumn("sentiment_label", F.lit("positive"))
        .withColumn("sentiment_score", F.lit(0.9).cast("float"))
    )


def test_agg_brand_daily_total_mentions(spark):
    agg = build_agg_brand_daily(_multi_brand_gold(spark))
    tesla_row = agg.filter(F.col("brand_name") == "Tesla").collect()
    assert len(tesla_row) == 1
    assert tesla_row[0]["total_mentions"] == 2


def test_agg_brand_daily_share_of_voice_sums_to_one(spark):
    agg = build_agg_brand_daily(_multi_brand_gold(spark))
    total_sov = agg.agg(F.sum("share_of_voice")).collect()[0][0]
    assert total_sov == pytest.approx(1.0, abs=1e-3)


def test_agg_brand_daily_positive_count(spark):
    agg = build_agg_brand_daily(_multi_brand_gold(spark))
    tesla_row = agg.filter(F.col("brand_name") == "Tesla").collect()[0]
    assert tesla_row["positive_count"] == 2


# ── validate_analytics ────────────────────────────────────────────────────────

def test_validate_analytics_passes_on_valid_data(spark):
    df = _enriched(spark)
    validate_analytics(df)  # should not raise


def test_validate_analytics_raises_on_null_engagement(spark):
    df = _enriched(spark).withColumn("engagement_score", F.lit(None).cast("double"))
    with pytest.raises(ValueError, match="invalid_engagement_score"):
        validate_analytics(df)


def test_validate_analytics_raises_on_null_is_viral(spark):
    df = _enriched(spark).withColumn("is_viral", F.lit(None).cast("boolean"))
    with pytest.raises(ValueError, match="null_is_viral"):
        validate_analytics(df)


# ── validate_gold ─────────────────────────────────────────────────────────────

def _build_all_gold(spark):
    gold_df = _gold_ready(spark)
    return (
        build_fact_tweets(gold_df),
        build_dim_brands(spark),
        build_dim_authors(gold_df),
        build_agg_brand_daily(gold_df),
        build_agg_author_perf(gold_df),
    )


def test_validate_gold_passes_on_valid_tables(spark):
    validate_gold(*_build_all_gold(spark))  # should not raise


def test_validate_gold_raises_on_null_tweet_id(spark):
    fact, dim_b, dim_a, agg_bd, agg_ap = _build_all_gold(spark)
    bad_fact = fact.withColumn("tweet_id", F.lit(None).cast("string"))
    with pytest.raises(ValueError, match="fact_tweets_null_keys"):
        validate_gold(bad_fact, dim_b, dim_a, agg_bd, agg_ap)


def test_validate_gold_raises_on_null_sentiment(spark):
    fact, dim_b, dim_a, agg_bd, agg_ap = _build_all_gold(spark)
    bad_fact = fact.withColumn("sentiment_label", F.lit(None).cast("string"))
    with pytest.raises(ValueError, match="fact_tweets_null_sentiment"):
        validate_gold(bad_fact, dim_b, dim_a, agg_bd, agg_ap)
