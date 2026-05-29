from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType, TimestampType

INSIGHT_SCHEMA = StructType([
    StructField("thread_id",           StringType(),    False),
    StructField("video_id",            StringType(),    False),
    StructField("entity",              StringType(),    False),
    StructField("aspect",              StringType(),    False),
    StructField("sentiment",           StringType(),    True),
    StructField("intensity",           StringType(),    True),
    StructField("mention_count",       IntegerType(),   True),
    StructField("evidence",            StringType(),    True),
    StructField("brand_comment_count", IntegerType(),   True),
    StructField("analyzed_at",         TimestampType(), True),
    StructField("year",                LongType(),      False),
    StructField("month",               LongType(),      False),
])
