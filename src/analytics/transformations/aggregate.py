from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# ── dim_brands static data ────────────────────────────────────────────────────

_BRANDS_DATA = [
    ("brand_01", "VinFast",    "VinFast Auto",   "Vietnam", "EV",      ["vinfast", "vfs", "vf3", "vf5", "vf6", "vf7", "vf8", "vf9"]),
    ("brand_02", "Tesla",      "Tesla, Inc.",     "USA",     "EV",      ["tesla"]),
    ("brand_03", "BYD",        "BYD Auto Co.",    "China",   "EV",      ["byd"]),
    ("brand_04", "Toyota",     "Toyota Motor",    "Japan",   "Hybrid",  ["toyota"]),
    ("brand_05", "BMW",        "BMW Group",       "Germany", "Luxury",  ["bmw"]),
    ("brand_06", "Mercedes",   "Mercedes-Benz",   "Germany", "Luxury",  ["mercedes"]),
    ("brand_07", "Hyundai",    "Hyundai Motor",   "South Korea", "General", ["hyundai"]),
    ("brand_08", "Ford",       "Ford Motor Co.",  "USA",     "General", ["ford"]),
    ("brand_09", "Porsche",    "Porsche AG",      "Germany", "Luxury",  ["porsche"]),
    ("brand_10", "Volkswagen", "Volkswagen AG",   "Germany", "General", ["volkswagen", "vw"]),
]

# ── Builders ──────────────────────────────────────────────────────────────────

def build_dim_brands(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        _BRANDS_DATA,
        schema=["brand_id", "brand_name", "brand_display_name",
                "brand_country", "brand_segment", "search_keywords"],
    )


def build_fact_tweets(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("id").alias("tweet_id"),
        F.col("author_id"),
        F.col("author_username"),
        F.col("author_followers"),
        F.col("primary_brand"),
        F.col("all_brands"),
        F.col("created_at_ts"),
        F.to_date(F.col("created_at_ts")).alias("date"),
        F.col("lang"),
        F.col("sentiment_score"),
        F.col("sentiment_label"),
        F.col("engagement_score"),
        F.col("is_viral"),
        F.col("is_bot"),
        F.col("hashtags"),
    )


def build_dim_authors(df: DataFrame) -> DataFrame:
    w_brand = Window.partitionBy("author_id").orderBy(F.col("_cnt").desc())
    top_brands = (
        df
        .withColumn("_brand", F.explode_outer(F.coalesce(F.col("all_brands"), F.array())))
        .filter(F.col("_brand").isNotNull())
        .groupBy("author_id", "_brand")
        .agg(F.count("*").alias("_cnt"))
        .withColumn("_rank", F.row_number().over(w_brand))
        .filter(F.col("_rank") <= 3)
        .groupBy("author_id")
        .agg(F.collect_list("_brand").alias("top_brands"))
    )

    base = (
        df.select(
            "author_id", "author_username", "author_is_blue_verified",
            "author_followers", "author_following", "author_statuses_count",
            "author_influence_ratio",
            F.col("is_bot").alias("is_bot_flag"),
        )
        .dropDuplicates(["author_id"])
    )

    return base.join(top_brands, on="author_id", how="left")


def build_agg_brand_daily(df: DataFrame) -> DataFrame:
    exploded = (
        df
        .withColumn("date", F.to_date(F.col("created_at_ts")))
        .withColumn("lang", F.coalesce(F.col("lang"), F.lit("unknown")))
        .withColumn("brand_name", F.explode_outer(F.coalesce(F.col("all_brands"), F.array())))
        .filter(F.col("brand_name").isNotNull())
    )

    agg = (
        exploded
        .groupBy("date", "brand_name", "lang")
        .agg(
            F.count("*").alias("total_mentions"),
            F.sum(F.when(F.lower(F.col("sentiment_label")) == "positive", 1).otherwise(0)).alias("positive_count"),
            F.sum(F.when(F.lower(F.col("sentiment_label")) == "neutral",  1).otherwise(0)).alias("neutral_count"),
            F.sum(F.when(F.lower(F.col("sentiment_label")) == "negative", 1).otherwise(0)).alias("negative_count"),
            F.avg("sentiment_score").alias("avg_sentiment_score"),
            F.avg("engagement_score").alias("avg_engagement_score"),
            F.sum(F.col("is_viral").cast("int")).alias("viral_count"),
            F.max(F.struct(F.col("engagement_score"), F.col("author_id")))["author_id"].alias("top_influencer_id"),
        )
    )

    w_sov = Window.partitionBy("date", "lang")
    return agg.withColumn(
        "share_of_voice",
        F.round(F.col("total_mentions") / F.sum("total_mentions").over(w_sov), 4),
    )


def build_agg_author_perf(df: DataFrame) -> DataFrame:
    w_sent = Window.partitionBy("author_id").orderBy(F.col("_sent_cnt").desc())
    dominant = (
        df.filter(F.col("sentiment_label").isNotNull())
        .groupBy("author_id", "sentiment_label")
        .agg(F.count("*").alias("_sent_cnt"))
        .withColumn("_rank", F.row_number().over(w_sent))
        .filter(F.col("_rank") == 1)
        .select("author_id", F.col("sentiment_label").alias("dominant_sentiment"))
    )

    w_brand = Window.partitionBy("author_id").orderBy(F.col("_cnt").desc())
    top_brands = (
        df
        .withColumn("_brand", F.explode_outer(F.coalesce(F.col("all_brands"), F.array())))
        .filter(F.col("_brand").isNotNull())
        .groupBy("author_id", "_brand")
        .agg(F.count("*").alias("_cnt"))
        .withColumn("_rank", F.row_number().over(w_brand))
        .filter(F.col("_rank") <= 5)
        .groupBy("author_id")
        .agg(F.collect_list("_brand").alias("top_brands"))
    )

    base = (
        df.groupBy("author_id")
        .agg(
            F.count("*").alias("total_tweets"),
            F.avg("engagement_score").alias("avg_engagement"),
            F.sum(F.col("is_viral").cast("int")).alias("viral_tweet_count"),
        )
    )

    return (
        base
        .join(top_brands, on="author_id", how="left")
        .join(dominant,   on="author_id", how="left")
    )
