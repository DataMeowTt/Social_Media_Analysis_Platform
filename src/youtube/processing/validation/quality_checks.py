from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.logger import get_logger

logger = get_logger(__name__)

def _temporal_invalid():
    return (
        F.col("comment_hour").isNull()        | (F.col("comment_hour") < 0)        | (F.col("comment_hour") > 23) |
        F.col("comment_day_of_week").isNull() | (F.col("comment_day_of_week") < 1) | (F.col("comment_day_of_week") > 7) |
        F.col("year").isNull()                | (F.col("year") <= 0)                |
        F.col("month").isNull()               | (F.col("month") <= 0)               | (F.col("month") > 12) |
        F.col("day").isNull()                 | (F.col("day") <= 0)                 | (F.col("day") > 31)
    )


def _check(df: DataFrame, condition, check_name: str) -> None:
    invalid = df.filter(condition)
    count = invalid.count()
    if count > 0:
        logger.error(f"[{check_name}] {count} rows failed. Sample:")
        for row in invalid.limit(5).collect():
            logger.error(f"  comment_id={row['comment_id']} | {row.asDict()}")
        raise ValueError(f"[{check_name}] {count} rows failed — see logs for details")


def assert_no_null_critical_fields(df: DataFrame) -> None:
    _check(
        df,
        F.col("comment_id").isNull() | F.col("text").isNull() | F.col("published_at").isNull(),
        "null_critical_fields",
    )


def assert_likes_non_negative(df: DataFrame) -> None:
    _check(df, F.col("likes") < 0, "negative_likes")


def assert_temporal_features_valid(df: DataFrame) -> None:
    _check(df, _temporal_invalid(), "invalid_temporal_features")


def validate_comments(df: DataFrame) -> None:
    invalid = df.filter(
        F.col("comment_id").isNull() | F.col("text").isNull() | F.col("published_at").isNull() |
        (F.col("likes") < 0) |
        _temporal_invalid()
    ).cache()

    try:
        count = invalid.count()
        if count == 0:
            return
        logger.error(f"[validation] {count} rows failed. Sample:")
        for row in invalid.limit(5).collect():
            logger.error(f"  comment_id={row['comment_id']} | {row.asDict()}")
        raise ValueError(f"[validation] {count} rows failed")
    finally:
        invalid.unpersist()
