from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType, TimestampType

VALID_MODELS = {"VF3", "VF5", "VF6", "VF7", "VF8", "VF9", "general"}
VALID_ASPECTS = {"pin", "sac", "bao_hanh", "phan_mem", "van_hanh", "gia", "loi_ky_thuat", "noi_that", "ket_noi"}

FB_INSIGHT_SCHEMA = StructType([
    StructField("post_id",        StringType(),    False),
    StructField("page_name",      StringType(),    False),
    StructField("model_entity",   StringType(),    False),
    StructField("aspect",         StringType(),    False),
    StructField("sentiment",      StringType(),    True),
    StructField("intensity",      StringType(),    True),
    StructField("mention_count",  IntegerType(),   True),
    StructField("evidence",       StringType(),    True),
    StructField("ingestion_date", DateType(),      True),
    StructField("analyzed_at",    TimestampType(), True),
])
