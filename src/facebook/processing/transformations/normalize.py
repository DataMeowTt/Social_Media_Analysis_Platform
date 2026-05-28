from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_SALE_PATTERN = (
    r"(?i)"
    r"\b(thanh[\s_]?lý|sang[\s_]?nhượng|chuyển[\s_]?nhượng|nhượng[\s_]?lại"
    r"|tìm[\s_]?mua|cần[\s_]?mua|cần[\s_]?bán|rao[\s_]?bán|đăng[\s_]?bán"
    r"|giá[\s_]?bán|giá[\s_]?xe|xe[\s_]?cũ|xe[\s_]?secondhand|second[\s_]?hand"
    r"|liên[\s_]?hệ[\s_]?mua|inbox[\s_]?mua|pm[\s_]?giá)\b"
)


def flatten_posts(df: DataFrame) -> DataFrame:
    text_col = F.coalesce(F.col("text"), F.lit(""))
    is_sale  = F.col("text").isNotNull() & (F.regexp_extract(text_col, _SALE_PATTERN, 0) != "")
    return (
        df.select(
            F.col("post_id"),
            F.col("source_type"),
            F.col("page_name"),
            F.col("text"),
            F.col("comment_count"),
            F.col("permalink"),
            F.to_date(F.col("ingestion_date")).alias("ingestion_date"),
        )
        .filter(~is_sale)
        .dropDuplicates(["post_id"])
    )


def flatten_comments(df: DataFrame) -> DataFrame:
    base = df.select(
        F.col("post_id"),
        F.col("page_name"),
        F.to_date(F.col("ingestion_date")).alias("ingestion_date"),
        F.explode_outer(F.col("comments")).alias("c"),
    )

    top_level = base.select(
        F.col("post_id"),
        F.col("page_name"),
        F.col("ingestion_date"),
        F.coalesce(F.col("c.author_id"),   F.lit("")).alias("author_id"),
        F.coalesce(F.col("c.author_name"), F.lit("")).alias("author_name"),
        F.coalesce(F.col("c.author_url"),  F.lit("")).alias("author_url"),
        F.coalesce(F.col("c.text"),        F.lit("")).alias("text"),
        F.coalesce(F.col("c.reaction_count").cast("int"), F.lit(0)).alias("reaction_count"),
        F.lit(None).cast("string").alias("parent_comment_id"),
        F.lit(False).alias("is_reply"),
        F.col("c.replies").alias("_replies"),
    ).withColumn(
        "comment_id",
        F.substring(
            F.sha2(F.concat_ws("|", F.col("post_id"), F.col("author_id"), F.col("text")), 256),
            1, 16,
        ),
    )

    replies = (
        top_level
        .filter(F.size(F.coalesce(F.col("_replies"), F.array())) > 0)
        .select(
            F.col("post_id"),
            F.col("page_name"),
            F.col("ingestion_date"),
            F.col("comment_id").alias("parent_comment_id"),
            F.explode(F.col("_replies")).alias("r"),
        )
        .select(
            F.col("post_id"),
            F.col("page_name"),
            F.col("ingestion_date"),
            F.coalesce(F.col("r.author_id"),   F.lit("")).alias("author_id"),
            F.coalesce(F.col("r.author_name"), F.lit("")).alias("author_name"),
            F.coalesce(F.col("r.author_url"),  F.lit("")).alias("author_url"),
            F.coalesce(F.col("r.text"),        F.lit("")).alias("text"),
            F.coalesce(F.col("r.reaction_count").cast("int"), F.lit(0)).alias("reaction_count"),
            F.col("parent_comment_id"),
            F.lit(True).alias("is_reply"),
        )
        .withColumn(
            "comment_id",
            F.substring(
                F.sha2(F.concat_ws("|", F.col("post_id"), F.col("author_id"), F.col("text")), 256),
                1, 16,
            ),
        )
    )

    cols = [
        "comment_id", "post_id", "page_name", "author_id", "author_name",
        "author_url", "text", "reaction_count", "parent_comment_id",
        "is_reply", "ingestion_date",
    ]
    return (
        top_level.drop("_replies").select(*cols)
        .unionByName(replies.select(*cols))
        .dropDuplicates(["comment_id"])
    )
