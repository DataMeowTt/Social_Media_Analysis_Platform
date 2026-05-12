from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, TimestampType, DateType, ArrayType,
)

FACT_TWEETS_SCHEMA = StructType([
    StructField("tweet_id",         StringType(),             False),
    StructField("author_id",        StringType(),             False),
    StructField("primary_brand",    StringType(),             True),
    StructField("all_brands",       ArrayType(StringType()), True),
    StructField("created_at_ts",    TimestampType(),          False),
    StructField("date",             DateType(),               False),
    StructField("lang",             StringType(),             True),
    StructField("sentiment_score",  DoubleType(),             True),
    StructField("sentiment_label",  StringType(),             True),
    StructField("engagement_score", DoubleType(),             True),
    StructField("is_viral",         BooleanType(),            True),
    StructField("is_bot",           BooleanType(),            True),
    StructField("hashtags",         ArrayType(StringType()), True),
])
