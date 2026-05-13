import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    IntegerType, BooleanType, ArrayType,
)

from src.processing.transformations.clean import clean_tweets
from src.processing.transformations.enrich import enrich_tweets
from src.processing.transformations.normalize import flatten_tweets
from src.processing.validation.quality_checks import validate_tweets
from src.processing.validation.schema import TWEET_SCHEMA

# ── Schema for flat tweets (output of flatten_tweets) ─────────────────────────

FLAT_SCHEMA = StructType([
    StructField("id",                    StringType(),          True),
    StructField("type",                  StringType(),          True),
    StructField("url",                   StringType(),          True),
    StructField("text",                  StringType(),          True),
    StructField("source",                StringType(),          True),
    StructField("lang",                  StringType(),          True),
    StructField("retweetCount",          LongType(),            True),
    StructField("replyCount",            LongType(),            True),
    StructField("likeCount",             LongType(),            True),
    StructField("quoteCount",            LongType(),            True),
    StructField("createdAt",             StringType(),          True),
    StructField("conversationId",        StringType(),          True),
    StructField("inReplyToId",           StringType(),          True),
    StructField("inReplyToUserId",       StringType(),          True),
    StructField("inReplyToUsername",     StringType(),          True),
    StructField("ingestion_date",        StringType(),          True),
    StructField("author_id",             StringType(),          True),
    StructField("author_username",       StringType(),          True),
    StructField("author_is_blue_verified", BooleanType(),       True),
    StructField("author_followers",      IntegerType(),         True),
    StructField("author_following",      IntegerType(),         True),
    StructField("author_statuses_count", IntegerType(),         True),
    StructField("author_createdAt",      StringType(),          True),
    StructField("hashtags",              ArrayType(StringType()), True),
    StructField("mentioned_user_ids",    ArrayType(StringType()), True),
])

_VALID_TS   = "Fri May 09 08:14:27 +0000 2026"
_VALID_AUTH_TS = "Mon Jan 01 00:00:00 +0000 2020"


def _flat_row(**overrides) -> tuple:
    defaults = {
        "id": "1", "type": "tweet", "url": None, "text": "hello world",
        "source": None, "lang": "en",
        "retweetCount": 10, "replyCount": 2, "likeCount": 5, "quoteCount": 1,
        "createdAt": _VALID_TS,
        "conversationId": None, "inReplyToId": None,
        "inReplyToUserId": None, "inReplyToUsername": None,
        "ingestion_date": "2026-05-09T08:14:27Z",
        "author_id": "auth1", "author_username": "user1",
        "author_is_blue_verified": False,
        "author_followers": 1000, "author_following": 500,
        "author_statuses_count": 1000,
        "author_createdAt": _VALID_AUTH_TS,
        "hashtags": None, "mentioned_user_ids": None,
    }
    d = {**defaults, **overrides}
    return tuple(d[f.name] for f in FLAT_SCHEMA.fields)


def _flat(spark, **overrides):
    return spark.createDataFrame([_flat_row(**overrides)], FLAT_SCHEMA)


def _raw_row(**overrides) -> dict:
    return {
        "type": "tweet", "id": "1", "url": None,
        "text": "hello", "source": None,
        "retweetCount": 0, "replyCount": 0, "likeCount": 0, "quoteCount": 0,
        "createdAt": _VALID_TS, "lang": "en",
        "inReplyToId": None, "conversationId": None,
        "inReplyToUserId": None, "inReplyToUsername": None,
        "author": {
            "userName": "user1", "id": "auth1", "isBlueVerified": False,
            "followers": 100, "following": 50, "statusesCount": 500,
            "createdAt": _VALID_AUTH_TS,
        },
        "entities": None, "quoted_tweet": None,
        "ingestion_date": "2026-05-09T08:14:27Z",
        **overrides,
    }


def _raw(spark, **overrides):
    return spark.createDataFrame([_raw_row(**overrides)], TWEET_SCHEMA)


# ── flatten_tweets ────────────────────────────────────────────────────────────

def test_flatten_extracts_author_fields(spark):
    df = flatten_tweets(_raw(spark))
    row = df.collect()[0]
    assert row["author_id"] == "auth1"
    assert row["author_username"] == "user1"


def test_flatten_deduplicates_by_id(spark):
    df = spark.createDataFrame(
        [_raw_row(id="1"), _raw_row(id="1"), _raw_row(id="2")],
        TWEET_SCHEMA,
    )
    result = flatten_tweets(df)
    ids = [r["id"] for r in result.collect()]
    assert sorted(ids) == ["1", "2"]


def test_flatten_none_entities_gives_null_hashtags(spark):
    df = flatten_tweets(_raw(spark))
    row = df.collect()[0]
    assert row["hashtags"] is None


# ── clean_tweets ─────────────────────────────────────────────────────────────

def test_clean_drops_null_id(spark):
    df = _flat(spark, id=None)
    assert clean_tweets(df).count() == 0


def test_clean_drops_null_text(spark):
    df = _flat(spark, text=None)
    assert clean_tweets(df).count() == 0


def test_clean_drops_null_author_id(spark):
    df = _flat(spark, author_id=None)
    assert clean_tweets(df).count() == 0


def test_clean_drops_null_created_at(spark):
    df = _flat(spark, createdAt=None)
    assert clean_tweets(df).count() == 0


def test_clean_drops_negative_retweet_count(spark):
    df = _flat(spark, retweetCount=-1)
    assert clean_tweets(df).count() == 0


def test_clean_drops_negative_like_count(spark):
    df = _flat(spark, likeCount=-5)
    assert clean_tweets(df).count() == 0


def test_clean_keeps_valid_row(spark):
    df = _flat(spark)
    assert clean_tweets(df).count() == 1


def test_clean_text_removes_mentions(spark):
    df = _flat(spark, text="@alice hello @bob world")
    result = clean_tweets(df).collect()[0]["text"]
    assert "@alice" not in result
    assert "@bob" not in result
    assert "hello" in result


def test_clean_text_removes_urls(spark):
    df = _flat(spark, text="check https://example.com now")
    result = clean_tweets(df).collect()[0]["text"]
    assert "https://" not in result
    assert "check" in result


# ── enrich_tweets ─────────────────────────────────────────────────────────────

def test_enrich_parses_valid_twitter_timestamp(spark):
    df = enrich_tweets(_flat(spark))
    ts = df.collect()[0]["created_at_ts"]
    assert ts is not None
    assert ts.year == 2026
    assert ts.month == 5
    assert ts.day == 9


def test_enrich_drops_unparseable_timestamp(spark):
    df = _flat(spark, createdAt="not-a-timestamp")
    # clean removes null author_createdAt etc, but we need a clean-like df
    # enrich_tweets first calls add_created_at_ts then drop_unparseable_timestamps
    assert enrich_tweets(df).count() == 0


def test_enrich_adds_temporal_features(spark):
    df = enrich_tweets(_flat(spark))
    row = df.collect()[0]
    assert row["year"] == 2026
    assert row["month"] == 5
    assert row["day"] == 9
    assert row["tweet_hour"] == 8
    assert 1 <= row["tweet_day_of_week"] <= 7


def test_enrich_is_reply_true_when_in_reply_to_id_set(spark):
    df = enrich_tweets(_flat(spark, inReplyToId="9999"))
    assert df.collect()[0]["is_reply"] is True


def test_enrich_is_reply_false_when_no_in_reply_to_id(spark):
    df = enrich_tweets(_flat(spark, inReplyToId=None))
    assert df.collect()[0]["is_reply"] is False


def test_enrich_is_retweet_flag(spark):
    df = enrich_tweets(_flat(spark, type="retweet"))
    assert df.collect()[0]["is_retweet"] is True


def test_enrich_hashtag_count_when_none(spark):
    df = enrich_tweets(_flat(spark, hashtags=None))
    assert df.collect()[0]["hashtag_count"] == 0


def test_enrich_hashtag_count_with_tags(spark):
    df = enrich_tweets(_flat(spark, hashtags=["tesla", "ev"]))
    assert df.collect()[0]["hashtag_count"] == 2


def test_enrich_mention_count_with_mentions(spark):
    df = enrich_tweets(_flat(spark, mentioned_user_ids=["u1", "u2", "u3"]))
    assert df.collect()[0]["mention_count"] == 3


# ── validate_tweets ───────────────────────────────────────────────────────────

def test_validate_tweets_passes_on_valid_data(spark):
    df = enrich_tweets(_flat(spark))
    validate_tweets(df)  # should not raise


def test_validate_tweets_raises_on_null_id(spark):
    df = enrich_tweets(_flat(spark)).withColumn("id", F.lit(None).cast("string"))
    with pytest.raises(ValueError, match="null_critical_fields"):
        validate_tweets(df)


def test_validate_tweets_raises_on_negative_engagement(spark):
    df = enrich_tweets(_flat(spark)).withColumn("retweetCount", F.lit(-1).cast("long"))
    with pytest.raises(ValueError, match="negative_engagement"):
        validate_tweets(df)
