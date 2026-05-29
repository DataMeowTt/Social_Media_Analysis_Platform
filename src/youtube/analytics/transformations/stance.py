import gc

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from src.utils.logger import get_logger

logger = get_logger(__name__)

SCHEMA = StructType([
    StructField("comment_id",   StringType(), True),
    StructField("video_id",     StringType(), True),
    StructField("parent_id",    StringType(), True),
    StructField("stance_label", StringType(), True),
])


def run_stance(spark: SparkSession, youtube_id: str) -> None:
    from src.storage.s3.reader import read_silver_youtube
    from src.storage.s3.uploader import write_to_s3
    from src.ml.inference.youtube_stance import predict_stance

    logger.info(f"[stance] Starting for {youtube_id}")

    pdf = read_silver_youtube(spark, youtube_id).select(
        "comment_id", "parent_id", "text"
    ).toPandas()

    replies = pdf[pdf["parent_id"].notna()].copy()
    parents = pdf[["comment_id", "text"]].rename(
        columns={"comment_id": "parent_id", "text": "parent_text"}
    )
    joined = replies.merge(parents, on="parent_id", how="inner")
    stance_labels = predict_stance(joined["text"].tolist(), joined["parent_text"].tolist())

    result_pdf = pd.DataFrame({
        "comment_id":   joined["comment_id"].values,
        "video_id":     youtube_id,
        "parent_id":    joined["parent_id"].values,
        "stance_label": stance_labels,
    })
    stance_df = spark.createDataFrame(result_pdf, schema=SCHEMA)
    del pdf, replies, parents, joined, stance_labels, result_pdf
    gc.collect()

    write_to_s3(
        stance_df,
        table_name="analytics.fct_comment_stance",
        layer="analytics",
        mode="overwrite",
        partition_cols=[],
        path_override="analytics/youtube/stance",
    )
    logger.info("[stance] Done — fct_comment_stance written")
