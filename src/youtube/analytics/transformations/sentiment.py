import gc
import re

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from src.utils.logger import get_logger

logger = get_logger(__name__)

_iphone_re = re.compile(r"(?i)\b(iphone|ios)\b")
_android_re = re.compile(r"(?i)\bandroid\b")

SCHEMA = StructType([
    StructField("comment_id",      StringType(),  True),
    StructField("video_id",        StringType(),  True),
    StructField("text",            StringType(),  True),
    StructField("brand_mentioned", StringType(),  True),
    StructField("sentiment",       StringType(),  True),
    StructField("author_name",     StringType(),  True),
    StructField("is_root",         BooleanType(), True),
    StructField("year",            LongType(),    True),
    StructField("month",           LongType(),    True),
])


def run_sentiment(spark: SparkSession, youtube_id: str) -> None:
    from src.storage.s3.reader import read_silver_youtube
    from src.storage.s3.uploader import write_to_s3
    from src.ml.inference.youtube_sentiment import predict_sentiment

    logger.info(f"[sentiment] Starting for {youtube_id}")

    pdf = read_silver_youtube(spark, youtube_id).select(
        "comment_id", "parent_id", "author", "text", "year", "month"
    ).toPandas()

    texts = pdf["text"].tolist()
    brand_lists = [
        [b for b, p in [("iPhone", _iphone_re), ("Android", _android_re)] if p.search(str(t))]
        for t in texts
    ]
    iphone_res, android_res = predict_sentiment(texts, brand_lists)

    rows = []
    for i in range(len(pdf)):
        for brand in brand_lists[i]:
            rows.append({
                "comment_id":      pdf["comment_id"].iloc[i],
                "video_id":        youtube_id,
                "text":            pdf["text"].iloc[i],
                "brand_mentioned": brand,
                "sentiment":       iphone_res[i] if brand == "iPhone" else android_res[i],
                "author_name":     pdf["author"].iloc[i],
                "is_root":         pd.isna(pdf["parent_id"].iloc[i]),
                "year":            pdf["year"].iloc[i],
                "month":           pdf["month"].iloc[i],
            })

    result_pdf = pd.DataFrame(rows, columns=[f.name for f in SCHEMA.fields])
    sentiment_df = spark.createDataFrame(result_pdf, schema=SCHEMA)
    del pdf, texts, brand_lists, iphone_res, android_res, rows, result_pdf
    gc.collect()

    write_to_s3(
        sentiment_df,
        table_name="analytics.fct_comment_sentiment",
        layer="analytics",
        mode="overwrite",
        partition_cols=["year", "month"],
        path_override="analytics/youtube/comments",
    )
    logger.info("[sentiment] Done — fct_comment_sentiment written")
