import math
import pytest
from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, IntegerType,
    StringType, StructField, StructType,
)

from src.facebook.analytics.transformations.aggregate import (
    build_agg_model_issue_weekly,
    build_agg_page_daily,
    build_dim_posts,
    build_fact_comments,
)
from src.facebook.analytics.transformations.enrich import add_engagement_score, add_is_bot, enrich_comments
from src.facebook.analytics.validate.quality_checks import validate_gold, validate_silver_comments

# ── Schemas ───────────────────────────────────────────────────────────────────

SILVER_COMMENT_SCHEMA = StructType([
    StructField("comment_id",        StringType(),  True),
    StructField("post_id",           StringType(),  True),
    StructField("page_name",         StringType(),  True),
    StructField("author_id",         StringType(),  True),
    StructField("author_name",       StringType(),  True),
    StructField("author_url",        StringType(),  True),
    StructField("text",              StringType(),  True),
    StructField("reaction_count",    IntegerType(), True),
    StructField("parent_comment_id", StringType(),  True),
    StructField("is_reply",          BooleanType(), True),
    StructField("ingestion_date",    DateType(),    True),
])

DIM_POST_SCHEMA = StructType([
    StructField("post_id",        StringType(),  True),
    StructField("page_name",      StringType(),  True),
    StructField("source_type",    StringType(),  True),
    StructField("text",           StringType(),  True),
    StructField("comment_count",  IntegerType(), True),
    StructField("sentiment_label",StringType(),  True),
    StructField("ingestion_date", DateType(),    True),
])

INSIGHT_SCHEMA = StructType([
    StructField("post_id",        StringType(),  True),
    StructField("page_name",      StringType(),  True),
    StructField("model_entity",   StringType(),  True),
    StructField("aspect",         StringType(),  True),
    StructField("sentiment",      StringType(),  True),
    StructField("intensity",      StringType(),  True),
    StructField("mention_count",  IntegerType(), True),
    StructField("ingestion_date", DateType(),    True),
])

_DATE = date(2026, 5, 1)


def _comment_row(comment_id="c1", post_id="p1", author_id="u1", is_reply=False, reaction_count=4, ingestion_date=_DATE, page_name="VinFast"):
    return (comment_id, post_id, page_name, author_id, "User", None, "text", reaction_count, None, is_reply, ingestion_date)


def _silver(spark, rows=None):
    rows = rows or [_comment_row()]
    return spark.createDataFrame(rows, SILVER_COMMENT_SCHEMA)


def _enriched(spark, rows=None):
    return enrich_comments(_silver(spark, rows))


def _gold_ready(spark, rows=None, sentiment="positive"):
    return (
        _enriched(spark, rows)
        .withColumn("sentiment_label", F.lit(sentiment))
        .withColumn("sentiment_score", F.lit(0.9).cast("float"))
    )


# ── add_engagement_score ──────────────────────────────────────────────────────

def test_engagement_score_root_comment(spark):
    df = add_engagement_score(_silver(spark, [_comment_row(is_reply=False, reaction_count=4)]))
    score = df.collect()[0]["engagement_score"]
    assert score == pytest.approx(4 * math.log(2), rel=1e-4)


def test_engagement_score_reply_higher_than_root(spark):
    root  = add_engagement_score(_silver(spark, [_comment_row(is_reply=False, reaction_count=4)])).collect()[0]["engagement_score"]
    reply = add_engagement_score(_silver(spark, [_comment_row(is_reply=True,  reaction_count=4)])).collect()[0]["engagement_score"]
    assert reply > root


def test_engagement_score_zero_reaction(spark):
    df = add_engagement_score(_silver(spark, [_comment_row(reaction_count=0)]))
    assert df.collect()[0]["engagement_score"] == 0.0


# ── add_is_bot ────────────────────────────────────────────────────────────────

def test_is_bot_true_above_threshold(spark):
    rows = [_comment_row(comment_id=str(i), author_id="spammer") for i in range(21)]
    df = add_is_bot(_silver(spark, rows))
    assert df.filter(F.col("author_id") == "spammer").collect()[0]["is_bot"] is True


def test_is_bot_false_below_threshold(spark):
    rows = [_comment_row(comment_id=str(i), author_id="normal") for i in range(5)]
    df = add_is_bot(_silver(spark, rows))
    assert df.filter(F.col("author_id") == "normal").collect()[0]["is_bot"] is False


def test_is_bot_partitioned_by_date(spark):
    day1 = [_comment_row(comment_id=str(i), author_id="u1", ingestion_date=date(2026, 5, 1)) for i in range(21)]
    day2 = [_comment_row(comment_id=str(i + 21), author_id="u1", ingestion_date=date(2026, 5, 2)) for i in range(1)]
    df = add_is_bot(_silver(spark, day1 + day2))
    day2_row = df.filter(F.col("ingestion_date") == date(2026, 5, 2)).collect()[0]
    assert day2_row["is_bot"] is False


# ── build_fact_comments ───────────────────────────────────────────────────────

def test_build_fact_comments_required_columns(spark):
    df = build_fact_comments(_gold_ready(spark))
    for col in ("comment_id", "post_id", "author_id", "sentiment_label", "sentiment_score", "engagement_score", "is_bot", "date", "page_name"):
        assert col in df.columns


def test_build_fact_comments_row_count(spark):
    rows = [_comment_row(comment_id=str(i)) for i in range(3)]
    assert build_fact_comments(_gold_ready(spark, rows)).count() == 3


# ── build_agg_page_daily ──────────────────────────────────────────────────────

def _multi_sentiment_gold(spark):
    rows = [
        _comment_row(comment_id="c1", post_id="p1", page_name="VinFast"),
        _comment_row(comment_id="c2", post_id="p1", page_name="VinFast"),
        _comment_row(comment_id="c3", post_id="p2", page_name="VinFast"),
    ]
    return (
        _enriched(spark, rows)
        .withColumn("sentiment_label",
            F.when(F.col("comment_id") == "c1", F.lit("positive"))
             .when(F.col("comment_id") == "c2", F.lit("negative"))
             .otherwise(F.lit("neutral")))
        .withColumn("sentiment_score", F.lit(0.8).cast("float"))
    )


def test_agg_page_daily_total_posts(spark):
    agg = build_agg_page_daily(_multi_sentiment_gold(spark))
    assert agg.collect()[0]["total_posts"] == 2


def test_agg_page_daily_total_comments(spark):
    agg = build_agg_page_daily(_multi_sentiment_gold(spark))
    assert agg.collect()[0]["total_comments"] == 3


def test_agg_page_daily_sentiment_counts(spark):
    agg = build_agg_page_daily(_multi_sentiment_gold(spark))
    row = agg.collect()[0]
    assert row["positive_count"] == 1
    assert row["negative_count"] == 1
    assert row["neutral_count"] == 1


def test_agg_page_daily_ratios_sum_le_one(spark):
    agg = build_agg_page_daily(_multi_sentiment_gold(spark))
    row = agg.collect()[0]
    assert row["positive_ratio"] + row["negative_ratio"] <= 1.0 + 1e-4


# ── build_agg_model_issue_weekly ──────────────────────────────────────────────

def _insight_df(spark, rows):
    return spark.createDataFrame(rows, INSIGHT_SCHEMA)


def test_agg_model_issue_weekly_week_start(spark):
    rows = [("post1", "VinFast", "VF8", "pin", "negative", "strong", 10, date(2026, 5, 6))]
    agg = build_agg_model_issue_weekly(_insight_df(spark, rows))
    week_start = agg.collect()[0]["week_start"]
    assert week_start == date(2026, 5, 4)  # Monday of that week


def test_agg_model_issue_weekly_trend_rising(spark):
    rows = [
        ("p1", "VF", "VF8", "pin", "negative", "strong", 5,  date(2026, 5, 4)),
        ("p2", "VF", "VF8", "pin", "negative", "strong", 10, date(2026, 5, 11)),
    ]
    agg = build_agg_model_issue_weekly(_insight_df(spark, rows))
    latest = agg.orderBy("week_start", ascending=False).collect()[0]
    assert latest["trend_direction"] == "rising"


def test_agg_model_issue_weekly_trend_falling(spark):
    rows = [
        ("p1", "VF", "VF8", "pin", "positive", "strong", 10, date(2026, 5, 4)),
        ("p2", "VF", "VF8", "pin", "positive", "mild",   3,  date(2026, 5, 11)),
    ]
    agg = build_agg_model_issue_weekly(_insight_df(spark, rows))
    latest = agg.orderBy("week_start", ascending=False).collect()[0]
    assert latest["trend_direction"] == "falling"


def test_agg_model_issue_weekly_trend_stable_first_week(spark):
    rows = [("p1", "VF", "VF8", "pin", "negative", "strong", 5, date(2026, 5, 4))]
    agg = build_agg_model_issue_weekly(_insight_df(spark, rows))
    assert agg.collect()[0]["trend_direction"] == "stable"


# ── build_dim_posts ───────────────────────────────────────────────────────────

def _dim_post_row(post_id="p1", text="hello", page_name="VinFast"):
    return (post_id, page_name, "group", text, 5, "positive", _DATE)


def test_build_dim_posts_dedup_by_post_id(spark):
    rows = [_dim_post_row("p1"), _dim_post_row("p1"), _dim_post_row("p2")]
    df = spark.createDataFrame(rows, DIM_POST_SCHEMA)
    assert build_dim_posts(df).count() == 2


def test_build_dim_posts_text_preview_max_200(spark):
    long_text = "x" * 300
    df = spark.createDataFrame([_dim_post_row(text=long_text)], DIM_POST_SCHEMA)
    preview = build_dim_posts(df).collect()[0]["text_preview"]
    assert len(preview) == 200


def test_build_dim_posts_null_text_preview_empty(spark):
    df = spark.createDataFrame([_dim_post_row(text=None)], DIM_POST_SCHEMA)
    preview = build_dim_posts(df).collect()[0]["text_preview"]
    assert preview == ""


# ── validate_silver_comments ──────────────────────────────────────────────────

def test_validate_silver_comments_passes(spark):
    validate_silver_comments(_silver(spark))  # should not raise


def test_validate_silver_comments_raises_null_comment_id(spark):
    df = _silver(spark).withColumn("comment_id", F.lit(None).cast("string"))
    with pytest.raises(ValueError, match="null comment_id"):
        validate_silver_comments(df)


def test_validate_silver_comments_raises_null_post_id(spark):
    df = _silver(spark).withColumn("post_id", F.lit(None).cast("string"))
    with pytest.raises(ValueError, match="null post_id"):
        validate_silver_comments(df)


# ── validate_gold ─────────────────────────────────────────────────────────────

def test_validate_gold_passes(spark):
    fact = build_fact_comments(_gold_ready(spark))
    agg  = build_agg_page_daily(_gold_ready(spark))
    validate_gold(fact, agg)  # should not raise


def test_validate_gold_raises_null_comment_id(spark):
    fact = build_fact_comments(_gold_ready(spark)).withColumn("comment_id", F.lit(None).cast("string"))
    agg  = build_agg_page_daily(_gold_ready(spark))
    with pytest.raises(ValueError, match="null comment_id"):
        validate_gold(fact, agg)


def test_validate_gold_raises_null_sentiment(spark):
    fact = build_fact_comments(_gold_ready(spark)).withColumn("sentiment_label", F.lit(None).cast("string"))
    agg  = build_agg_page_daily(_gold_ready(spark))
    with pytest.raises(ValueError, match="null sentiment"):
        validate_gold(fact, agg)


def test_validate_gold_raises_duplicate_comment_id(spark):
    fact = build_fact_comments(_gold_ready(spark))
    dup  = fact.unionByName(fact)
    agg  = build_agg_page_daily(_gold_ready(spark))
    with pytest.raises(ValueError, match="duplicate"):
        validate_gold(dup, agg)
