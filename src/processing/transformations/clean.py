from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.logger import get_logger

logger = get_logger(__name__)

def drop_missing_critical_fields(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("id").isNotNull() &
        F.col("text").isNotNull() &
        F.col("createdAt").isNotNull() &
        F.col("author_id").isNotNull()
    )

def drop_negative_engagement(df: DataFrame) -> DataFrame:
    return df.filter(
        (F.col("retweetCount") >= 0) &
        (F.col("likeCount")    >= 0) &
        (F.col("replyCount")   >= 0) &
        (F.col("quoteCount")   >= 0)
    )

def parse_source(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "source",
        F.regexp_extract(F.col("source"), r">(.+?)</a>", 1)
    )

def clean_text(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("text", F.regexp_replace(F.col("text"), r"@\w+", "")) # Remove mentions
        .withColumn("text", F.regexp_replace(F.col("text"), r"https?://\S+", "")) # Remove URLs
        .withColumn("text", F.regexp_replace(F.col("text"), r"[^\w\s#\U0001F300-\U0010FFFF]", "")) # Remove special characters except hashtags and emojis
        .withColumn("text", F.trim(F.regexp_replace(F.col("text"), r"\s{2,}", " "))) # Replace multiple spaces with single space and trim
    )

def clean_tweets(df: DataFrame) -> DataFrame:
    df = drop_missing_critical_fields(df)
    df = drop_negative_engagement(df)
    df = parse_source(df)
    df = clean_text(df)
    
    return df