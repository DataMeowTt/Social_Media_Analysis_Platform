from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.helpers import clean_text
from src.utils.logger import get_logger

logger = get_logger(__name__)


def drop_missing_critical_fields(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("comment_id").isNotNull() &
        F.col("text").isNotNull() &
        F.col("published_at").isNotNull()
    )


def drop_invalid_likes(df: DataFrame) -> DataFrame:
    return df.filter(F.col("likes") >= 0)


def clean_comments(df: DataFrame) -> DataFrame:
    df = drop_missing_critical_fields(df)
    df = drop_invalid_likes(df)
    df = clean_text(df)
    return df
