import pytest
from pyspark.sql import functions as F

from src.youtube.processing.transformations.clean import drop_invalid_likes, drop_missing_critical_fields
from src.youtube.processing.transformations.enrich import add_published_at_ts, add_temporal_features, drop_unparseable_timestamps, enrich_comments
from src.youtube.processing.validation.quality_checks import validate_comments
from src.youtube.processing.validation.schema import COMMENT_SCHEMA

_VALID_TS = "2026-05-09T08:14:27Z"


def _row(comment_id="c1", text="hello", published_at=_VALID_TS, likes=5, is_reply=False):
    return (comment_id, None, "Author", text, likes, published_at, is_reply)


def _df(spark, rows=None):
    rows = rows or [_row()]
    return spark.createDataFrame(rows, COMMENT_SCHEMA)


# ── drop_missing_critical_fields ──────────────────────────────────────────────

def test_drop_missing_null_comment_id(spark):
    df = _df(spark, [_row(comment_id=None)])
    assert drop_missing_critical_fields(df).count() == 0


def test_drop_missing_null_text(spark):
    df = _df(spark, [_row(text=None)])
    assert drop_missing_critical_fields(df).count() == 0


def test_drop_missing_null_published_at(spark):
    df = _df(spark, [_row(published_at=None)])
    assert drop_missing_critical_fields(df).count() == 0


def test_drop_missing_keeps_valid_row(spark):
    assert drop_missing_critical_fields(_df(spark)).count() == 1


# ── drop_invalid_likes ────────────────────────────────────────────────────────

def test_drop_invalid_likes_negative(spark):
    df = _df(spark, [_row(likes=-1)])
    assert drop_invalid_likes(df).count() == 0


def test_drop_invalid_likes_zero_kept(spark):
    df = _df(spark, [_row(likes=0)])
    assert drop_invalid_likes(df).count() == 1


# ── add_published_at_ts ───────────────────────────────────────────────────────

def test_add_published_at_ts_valid(spark):
    df = add_published_at_ts(_df(spark))
    ts = df.collect()[0]["published_at_ts"]
    assert ts is not None
    assert ts.year == 2026
    assert ts.month == 5
    assert ts.day == 9


def test_add_published_at_ts_invalid_string_gives_null(spark):
    df = add_published_at_ts(_df(spark, [_row(published_at="not-a-date")]))
    assert df.collect()[0]["published_at_ts"] is None


# ── drop_unparseable_timestamps ───────────────────────────────────────────────

def test_drop_unparseable_timestamps(spark):
    df = add_published_at_ts(_df(spark, [_row(published_at="bad")]))
    assert drop_unparseable_timestamps(df).count() == 0


# ── add_temporal_features ─────────────────────────────────────────────────────

def test_temporal_features_values(spark):
    df = add_temporal_features(add_published_at_ts(_df(spark)))
    row = df.collect()[0]
    assert row["year"]  == 2026
    assert row["month"] == 5
    assert row["day"]   == 9
    assert row["comment_hour"] == 8
    assert 1 <= row["comment_day_of_week"] <= 7


# ── enrich_comments ───────────────────────────────────────────────────────────

def test_enrich_comments_valid_row_passes(spark):
    df = enrich_comments(_df(spark))
    assert df.count() == 1


def test_enrich_comments_drops_bad_timestamp(spark):
    df = enrich_comments(_df(spark, [_row(published_at="garbage")]))
    assert df.count() == 0


# ── validate_comments ─────────────────────────────────────────────────────────

def test_validate_comments_passes(spark):
    df = enrich_comments(_df(spark))
    validate_comments(df)  # should not raise


def test_validate_comments_raises_null_comment_id(spark):
    df = enrich_comments(_df(spark)).withColumn("comment_id", F.lit(None).cast("string"))
    with pytest.raises(ValueError):
        validate_comments(df)


def test_validate_comments_raises_negative_likes(spark):
    df = enrich_comments(_df(spark)).withColumn("likes", F.lit(-1).cast("long"))
    with pytest.raises(ValueError):
        validate_comments(df)


def test_validate_comments_raises_invalid_temporal(spark):
    df = enrich_comments(_df(spark)).withColumn("comment_hour", F.lit(25))
    with pytest.raises(ValueError):
        validate_comments(df)
