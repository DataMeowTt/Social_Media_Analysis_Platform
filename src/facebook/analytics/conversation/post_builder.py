from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.storage.s3.reader import read_latest_silver_facebook
from src.utils.config_loader import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)
_config = load_config()
_BUCKET = _config["s3"]["bucket_name"]

_MIN_COMMENTS  = 5
_INSIGHTS_PATH = f"s3a://{_BUCKET}/analytics/facebook/fct_post_insights/"


def _already_analyzed(spark: SparkSession) -> set[str]:
    try:
        existing = spark.read.parquet(_INSIGHTS_PATH)
        return {row.post_id for row in existing.select("post_id").distinct().collect()}
    except Exception:
        return set()


def build_post_contexts(spark: SparkSession, force: bool = False) -> list[dict]:
    posts_df    = read_latest_silver_facebook(spark, "posts")
    comments_df = read_latest_silver_facebook(spark, "comments")

    eligible = posts_df.filter(F.col("comment_count") >= _MIN_COMMENTS)

    if not force:
        analyzed = _already_analyzed(spark)
        if analyzed:
            eligible = eligible.filter(~F.col("post_id").isin(analyzed))
            logger.info(f"[post_builder] skipping {len(analyzed)} already-analyzed posts")

    eligible_ids = {row.post_id for row in eligible.select("post_id").collect()}
    if not eligible_ids:
        logger.info("[post_builder] no eligible posts found")
        return []

    posts_pdf = eligible.select(
        "post_id", "page_name", "comment_count", "ingestion_date"
    ).toPandas()

    comments_pdf = (
        comments_df
        .filter(F.col("post_id").isin(eligible_ids))
        .select("post_id", "author_name", "text", "is_reply", "parent_comment_id")
        .toPandas()
    )

    contexts = []
    for _, post in posts_pdf.iterrows():
        post_comments = comments_pdf[comments_pdf["post_id"] == post["post_id"]]
        comment_list  = post_comments.to_dict("records")
        comment_list.sort(key=lambda c: bool(c.get("is_reply")))
        contexts.append({
            "post_id":        post["post_id"],
            "page_name":      post["page_name"],
            "comment_count":  int(post["comment_count"]),
            "ingestion_date": post["ingestion_date"],
            "comments":       comment_list,
        })

    logger.info(f"[post_builder] built {len(contexts)} post contexts")
    return contexts
