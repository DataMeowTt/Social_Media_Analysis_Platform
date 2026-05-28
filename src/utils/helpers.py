from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def parse_twitter_timestamp(col: Column) -> Column:
    # "Fri May 09 08:14:27 +0000 2026" → "2026-May-09 08:14:27"
    # EEE and trailing-year format is not supported by Spark 3.0+ DateTimeFormatter
    reformatted = F.regexp_replace(
        col,
        r'^\w+\s+(\w+)\s+(\d+)\s+(\d+:\d+:\d+)\s+\S+\s+(\d+)$',
        '$4-$1-$2 $3',
    )
    return F.to_timestamp(reformatted, "yyyy-MMM-d HH:mm:ss")


def clean_text(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("text", F.regexp_replace(F.col("text"), r"@\w+", ""))
        .withColumn("text", F.regexp_replace(F.col("text"), r"https?://\S+", ""))
        .withColumn("text", F.regexp_replace(F.col("text"), "[^\w\s#\U0001F300-\U0010FFFF]", ""))
        .withColumn("text", F.trim(F.regexp_replace(F.col("text"), r"\s{2,}", " ")))
    )

