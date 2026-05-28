from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def add_engagement_score(df: DataFrame) -> DataFrame:
    thread_depth = F.when(F.col("is_reply"), F.lit(1)).otherwise(F.lit(0))
    score = F.coalesce(F.col("reaction_count"), F.lit(0)).cast("float") * F.log(thread_depth + F.lit(2))
    return df.withColumn("engagement_score", score)


def add_is_bot(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("author_id", "ingestion_date")
    return (
        df
        .withColumn("_cnt", F.count("comment_id").over(w))
        .withColumn("is_bot", F.col("_cnt") > 20)
        .drop("_cnt")
    )


def enrich_comments(df: DataFrame) -> DataFrame:
    df = add_engagement_score(df)
    df = add_is_bot(df)
    return df
