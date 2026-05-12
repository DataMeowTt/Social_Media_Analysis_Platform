from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, DateType, ArrayType,
)

AGG_BRAND_DAILY_SCHEMA = StructType([
    StructField("date",                 DateType(),   False),
    StructField("brand_name",           StringType(), False),
    StructField("lang",                 StringType(), False),
    StructField("total_mentions",       LongType(),   True),
    StructField("positive_count",       LongType(),   True),
    StructField("neutral_count",        LongType(),   True),
    StructField("negative_count",       LongType(),   True),
    StructField("avg_sentiment_score",  DoubleType(), True),
    StructField("avg_engagement_score", DoubleType(), True),
    StructField("viral_count",          LongType(),   True),
    StructField("share_of_voice",       DoubleType(), True),
    StructField("top_influencer_id",    StringType(), True),
])

AGG_AUTHOR_PERF_SCHEMA = StructType([
    StructField("author_id",          StringType(),            False),
    StructField("total_tweets",       LongType(),              True),
    StructField("avg_engagement",     DoubleType(),            True),
    StructField("viral_tweet_count",  LongType(),              True),
    StructField("top_brands",         ArrayType(StringType()), True),
    StructField("dominant_sentiment", StringType(),            True),
])
