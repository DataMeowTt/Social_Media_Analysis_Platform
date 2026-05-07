from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.helpers import parse_twitter_timestamp
from src.utils.logger import get_logger

logger = get_logger(__name__)


def add_created_at_ts(df: DataFrame) -> DataFrame:
    return df.withColumn("created_at_ts", parse_twitter_timestamp(F.col("createdAt")))


def add_author_created_at_ts(df: DataFrame) -> DataFrame:
    return df.withColumn("author_created_at_ts", parse_twitter_timestamp(F.col("author_createdAt")))


def drop_unparseable_timestamps(df: DataFrame) -> DataFrame:
    before = df.count()
    df = df.filter(F.col("created_at_ts").isNotNull())
    dropped = before - df.count()
    if dropped > 0:
        logger.warning(f"Dropped {dropped} rows: could not parse createdAt timestamp")
    return df


def add_temporal_features(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("tweet_hour",       F.hour(F.col("created_at_ts")))
        .withColumn("tweet_day_of_week", F.dayofweek(F.col("created_at_ts")))
        .withColumn("year",             F.year(F.col("created_at_ts")))
        .withColumn("month",            F.month(F.col("created_at_ts")))
        .withColumn("day",              F.dayofmonth(F.col("created_at_ts")))
    )


def add_tweet_type_flags(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("is_reply",   F.col("inReplyToId").isNotNull())
        .withColumn("is_retweet", F.col("type") == "retweet")
    )


def add_entity_count(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("hashtag_count", F.size(F.coalesce(F.col("hashtags"), F.array())))
        .withColumn("mention_count",  F.size(F.coalesce(F.col("mentioned_user_ids"), F.array())))
    )


def enrich_tweets(df: DataFrame) -> DataFrame:
    df = add_created_at_ts(df)
    df = add_author_created_at_ts(df)
    df = drop_unparseable_timestamps(df)
    df = add_temporal_features(df)
    df = add_tweet_type_flags(df)
    df = add_entity_count(df)
    return df
