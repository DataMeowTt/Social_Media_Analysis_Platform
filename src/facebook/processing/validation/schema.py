from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

_REPLY_SCHEMA = StructType([
    StructField("author_id",      StringType(), True),
    StructField("author_name",    StringType(), True),
    StructField("author_url",     StringType(), True),
    StructField("text",           StringType(), True),
    StructField("reaction_count", StringType(), True),
])

_COMMENT_SCHEMA = StructType([
    StructField("author_id",         StringType(),             True),
    StructField("author_name",       StringType(),             True),
    StructField("author_url",        StringType(),             True),
    StructField("text",              StringType(),             True),
    StructField("reaction_count",    StringType(),             True),
    StructField("parent_comment_id", StringType(),             True),
    StructField("replies",           ArrayType(_REPLY_SCHEMA), True),
])

FB_POST_SCHEMA = StructType([
    StructField("post_id",        StringType(),               True),
    StructField("source_type",    StringType(),               True),
    StructField("page_name",      StringType(),               True),
    StructField("text",           StringType(),               True),
    StructField("feedback_id",    StringType(),               True),
    StructField("comment_count",  IntegerType(),              True),
    StructField("permalink",      StringType(),               True),
    StructField("ingestion_date", StringType(),               True),
    StructField("comments",       ArrayType(_COMMENT_SCHEMA), True),
])
