from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

COMMENT_SCHEMA = StructType([
    StructField("comment_id",   StringType(),  True),
    StructField("parent_id",    StringType(),  True),
    StructField("author",       StringType(),  True),
    StructField("text",         StringType(),  True),
    StructField("likes",        LongType(),    True),
    StructField("published_at", StringType(),  True),
    StructField("is_reply",     BooleanType(), True),
])
