from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column

# ── Brand catalogue ───────────────────────────────────────────────────────────

_BRAND_KEYWORDS = {
    "VinFast":    ["vinfast", "vfs", "vf3", "vf5", "vf6", "vf7", "vf8", "vf9"],
    "Tesla":      ["tesla"],
    "BYD":        ["byd"],
    "Toyota":     ["toyota"],
    "BMW":        ["bmw"],
    "Mercedes":   ["mercedes"],
    "Hyundai":    ["hyundai"],
    "Ford":       ["ford"],
    "Porsche":    ["porsche"],
    "Volkswagen": ["volkswagen"],
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _brand_match(keywords: list[str]) -> Column:
    text_lower = F.lower(F.col("text"))
    tags_lower = F.transform(
        F.coalesce(F.col("hashtags"), F.array()),
        lambda t: F.lower(t),
    )
    text_hit = F.lit(False)
    for kw in keywords:
        text_hit = text_hit | F.coalesce(text_lower.contains(kw), F.lit(False))

    tag_hit = F.lit(False)
    for kw in keywords:
        tag_hit = tag_hit | F.array_contains(tags_lower, kw)

    return text_hit | tag_hit

# ── Enrichment steps ──────────────────────────────────────────────────────────

def add_engagement_score(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "engagement_score",
        F.col("retweetCount") * 2
        + F.col("likeCount")
        + F.col("replyCount") * 1.5
        + F.col("quoteCount") * 1.5,
    )


def add_is_viral(df: DataFrame, threshold: int = 1000) -> DataFrame:
    return df.withColumn("is_viral", F.col("engagement_score") >= threshold)


def add_author_influence(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "author_influence_ratio",
        F.when(
            F.col("author_following") > 0,
            F.col("author_followers") / F.col("author_following"),
        ).otherwise(None),
    )


def add_brands(df: DataFrame) -> DataFrame:
    brand_cols = [
        F.when(_brand_match(kws), F.lit(brand))
        for brand, kws in _BRAND_KEYWORDS.items()
    ]
    df = df.withColumn("_brands_raw", F.array(*brand_cols))
    df = df.withColumn(
        "all_brands",
        F.expr("filter(_brands_raw, x -> x is not null)"),
    )
    df = df.withColumn(
        "primary_brand",
        F.coalesce(F.col("all_brands")[0], F.lit("Unknown")),
    )
    return df.drop("_brands_raw")

# TODO: change later
def add_is_bot(df: DataFrame) -> DataFrame:
    """Heuristic: very active account with very few followers."""
    return df.withColumn(
        "is_bot",
        (F.col("author_statuses_count") > 50_000) & (F.col("author_followers") < 500),
    )


def enrich_analytics(df: DataFrame) -> DataFrame:
    df = add_engagement_score(df)
    df = add_is_viral(df)
    df = add_author_influence(df)
    df = add_brands(df)
    df = add_is_bot(df)
    return df
