from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.helpers import clean_text
from src.utils.logger import get_logger

logger = get_logger(__name__)

def drop_missing_critical_fields(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("id").isNotNull() &
        F.col("text").isNotNull() &
        F.col("createdAt").isNotNull() &
        F.col("author_id").isNotNull() &
        F.col("author_createdAt").isNotNull()
    )

def drop_negative_engagement(df: DataFrame) -> DataFrame:
    return df.filter(
        (F.col("retweetCount") >= 0) &
        (F.col("likeCount")    >= 0) &
        (F.col("replyCount")   >= 0) &
        (F.col("quoteCount")   >= 0)
    )

def clean_tweets(df: DataFrame) -> DataFrame:
    df = drop_missing_critical_fields(df)
    df = drop_negative_engagement(df)
    df = clean_text(df)
    return df