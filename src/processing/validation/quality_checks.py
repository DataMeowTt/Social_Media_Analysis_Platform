from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _check(df: DataFrame, condition, check_name: str) -> None:
    invalid = df.filter(condition)
    count = invalid.count()
    if count > 0:
        logger.error(f"[{check_name}] {count} rows failed. Sample:")
        for row in invalid.limit(5).collect():
            logger.error(f"  id={row['id']} | {row.asDict()}")
        raise ValueError(f"[{check_name}] {count} rows failed — see logs for details")


def assert_no_null_critical_fields(df: DataFrame) -> None:
    _check(
        df,
        F.col("id").isNull() | F.col("text").isNull() |
        F.col("createdAt").isNull() | F.col("author_id").isNull() | F.col("author_createdAt").isNull(),
        "null_critical_fields"
    )


def assert_engagement_non_negative(df: DataFrame) -> None:
    _check(
        df,
        (F.col("retweetCount") < 0) | (F.col("likeCount") < 0) |
        (F.col("replyCount") < 0)   | (F.col("quoteCount") < 0),
        "negative_engagement"
    )


def assert_numeric_features_valid(df: DataFrame) -> None:
    _check(
        df,
        (F.col("hashtag_count") < 0) | (F.col("mention_count") < 0) |
        F.col("tweet_hour").isNull()        | (F.col("tweet_hour") < 0)        |
        F.col("tweet_day_of_week").isNull() | (F.col("tweet_day_of_week") < 1) |
        F.col("year").isNull()              | (F.col("year") <= 0)              |
        F.col("month").isNull()             | (F.col("month") <= 0)             |
        F.col("day").isNull()               | (F.col("day") <= 0),
        "invalid_numeric_features"
    )


def validate_tweets(df: DataFrame) -> None:
    assert_no_null_critical_fields(df)
    assert_engagement_non_negative(df)
    assert_numeric_features_valid(df)
