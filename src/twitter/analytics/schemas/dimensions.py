from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType, ArrayType,
)

DIM_BRANDS_SCHEMA = StructType([
    StructField("brand_id",           StringType(),            False),
    StructField("brand_name",         StringType(),            False),
    StructField("brand_display_name", StringType(),            True),
    StructField("brand_country",      StringType(),            True),
    StructField("brand_segment",      StringType(),            True),
    StructField("search_keywords",    ArrayType(StringType()), True),
])

DIM_AUTHORS_SCHEMA = StructType([
    StructField("author_id",               StringType(),            False),
    StructField("author_username",         StringType(),            True),
    StructField("author_is_blue_verified", BooleanType(),           True),
    StructField("author_followers",        IntegerType(),           True),
    StructField("author_following",        IntegerType(),           True),
    StructField("author_statuses_count",   IntegerType(),           True),
    StructField("author_influence_ratio",  DoubleType(),            True),
    StructField("top_brands",             ArrayType(StringType()), True),
    StructField("is_bot_flag",             BooleanType(),           True),
])
