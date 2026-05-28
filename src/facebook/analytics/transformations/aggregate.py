from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_fact_comments(df: DataFrame) -> DataFrame:
    return df.select(
        "comment_id",
        "post_id",
        "author_id",
        "text",
        "reaction_count",
        "is_reply",
        "parent_comment_id",
        "engagement_score",
        "is_bot",
        "sentiment_label",
        "sentiment_score",
        F.col("ingestion_date").alias("date"),
        "page_name",
    )


def build_agg_page_daily(df: DataFrame) -> DataFrame:
    agg = df.withColumn("date", F.col("ingestion_date")).groupBy("date", "page_name").agg(
        F.countDistinct("post_id").alias("total_posts"),
        F.count("comment_id").alias("total_comments"),
        F.sum(F.col("is_reply").cast("int")).alias("total_replies"),
        F.sum(F.when(F.col("sentiment_label") == "positive", 1).otherwise(0)).alias("positive_count"),
        F.sum(F.when(F.col("sentiment_label") == "negative", 1).otherwise(0)).alias("negative_count"),
        F.sum(F.when(F.col("sentiment_label") == "neutral",  1).otherwise(0)).alias("neutral_count"),
        F.round(F.avg(F.col("reaction_count").cast("float")), 2).alias("avg_reaction_count"),
    )
    total = F.col("total_comments")
    return agg.withColumn(
        "positive_ratio", F.round(F.col("positive_count") / total, 4),
    ).withColumn(
        "negative_ratio", F.round(F.col("negative_count") / total, 4),
    )


def build_agg_model_issue_weekly(df: DataFrame) -> DataFrame:
    df = df.withColumn("week_start", F.date_trunc("week", F.col("ingestion_date")).cast("date"))

    intensity_score = (
        F.when(F.col("intensity") == "strong",   F.lit(3))
         .when(F.col("intensity") == "moderate", F.lit(2))
         .otherwise(F.lit(1))
    )

    agg = df.groupBy("week_start", "model_entity", "aspect").agg(
        F.sum("mention_count").alias("total_mentions"),
        F.sum(F.when(F.col("sentiment") == "positive", F.col("mention_count")).otherwise(0)).alias("_pos"),
        F.sum(F.when(F.col("sentiment") == "negative", F.col("mention_count")).otherwise(0)).alias("_neg"),
        F.round(F.avg(intensity_score), 2).alias("avg_intensity_score"),
    ).withColumn(
        "positive_ratio", F.round(F.col("_pos") / F.col("total_mentions"), 4),
    ).withColumn(
        "negative_ratio", F.round(F.col("_neg") / F.col("total_mentions"), 4),
    ).drop("_pos", "_neg")

    w = Window.partitionBy("model_entity", "aspect").orderBy("week_start")
    agg = agg.withColumn("prev_week_mentions", F.lag("total_mentions", 1).over(w))

    return agg.withColumn(
        "trend_direction",
        F.when(
            (F.col("total_mentions") > F.col("prev_week_mentions") * 1.3)
            & (F.col("negative_ratio") > 0.5),
            F.lit("rising"),
        ).when(
            F.col("total_mentions") < F.col("prev_week_mentions") * 0.7,
            F.lit("falling"),
        ).otherwise(F.lit("stable")),
    )


def build_dim_posts(df: DataFrame) -> DataFrame:
    return df.select(
        "post_id",
        "page_name",
        "source_type",
        "text",
        F.substring(F.coalesce(F.col("text"), F.lit("")), 1, 200).alias("text_preview"),
        "comment_count",
        "sentiment_label",
        "ingestion_date",
    ).dropDuplicates(["post_id"])
