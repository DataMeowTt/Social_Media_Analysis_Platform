from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def assert_no_null_critical_fields(df: DataFrame) -> None:
    invalid = df.filter(
        F.col("id").isNull()        | 
        F.col("text").isNull()      |
        F.col("createdAt").isNull() |
        F.col("author_id").isNull()
    ).limit(1)
    assert invalid.isEmpty(), "Found rows with null critical fields"


def assert_engagement_non_negative(df: DataFrame) -> None:
    invalid = df.filter(
        (F.col("retweetCount") < 0) | 
        (F.col("likeCount")    < 0) |
        (F.col("replyCount")   < 0) |
        (F.col("quoteCount")   < 0)
    ).limit(1)
    assert invalid.isEmpty(), "Found rows with negative engagement counts"


def validate_tweets(df: DataFrame) -> None:              
    assert_no_null_critical_fields(df)  
    assert_engagement_non_negative(df)   
