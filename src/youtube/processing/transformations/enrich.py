from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_published_at_ts(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "published_at_ts",
        F.to_timestamp(F.col("published_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    )


def drop_unparseable_timestamps(df: DataFrame) -> DataFrame:
    return df.filter(F.col("published_at_ts").isNotNull())


def add_temporal_features(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("comment_hour",        F.hour(F.col("published_at_ts")))
        .withColumn("comment_day_of_week", F.dayofweek(F.col("published_at_ts")))
        .withColumn("year",                F.year(F.col("published_at_ts")))
        .withColumn("month",               F.month(F.col("published_at_ts")))
        .withColumn("day",                 F.dayofmonth(F.col("published_at_ts")))
    )


def enrich_comments(df: DataFrame) -> DataFrame:
    df = add_published_at_ts(df)
    df = drop_unparseable_timestamps(df)
    df = add_temporal_features(df)
    return df
