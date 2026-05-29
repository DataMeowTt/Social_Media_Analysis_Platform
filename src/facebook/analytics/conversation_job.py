from __future__ import annotations

import gc
import os
from datetime import datetime, timezone

import pandas as pd

from src.facebook.analytics.conversation.gemini_extractor import extract_insights_for_post
from src.facebook.analytics.conversation.insight_merger import merge_insights
from src.facebook.analytics.conversation.schemas.insight_schema import FB_INSIGHT_SCHEMA
from src.facebook.analytics.conversation.post_builder import build_post_contexts
from src.facebook.analytics.transformations.aggregate import build_agg_model_issue_weekly
from src.storage.s3.uploader import write_to_s3
from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.utils.session import create_spark_session

_BUCKET = load_config()["s3"]["bucket_name"]

logger = get_logger(__name__)

_FORCE = os.environ.get("FORCE_REANALYZE", "").lower() in ("1", "true", "yes")


def run_gemini_analysis() -> None:
    spark = create_spark_session("Facebook-Gemini-Analysis")
    try:
        post_contexts = build_post_contexts(spark, force=_FORCE)
    finally:
        spark.stop()

    if not post_contexts:
        logger.info("[conversation_job] no posts to analyze — done")
        return

    analyzed_at = datetime.now(timezone.utc)
    all_rows: list[dict] = []
    total = len(post_contexts)

    for idx, ctx in enumerate(post_contexts, start=1):
        logger.info(f"[conversation_job] post {idx}/{total} — {ctx['post_id']} ({ctx['comment_count']} comments)")
        raw    = extract_insights_for_post(ctx["comments"])
        merged = merge_insights(raw, ctx["post_id"], ctx["page_name"], ctx["ingestion_date"], analyzed_at)
        all_rows.extend(merged)
        logger.info(f"[conversation_job] post {idx}/{total} done — {len(merged)} insights, total: {len(all_rows)}")
        gc.collect()

    if not all_rows:
        logger.info("[conversation_job] no insights generated — done")
        return

    spark = create_spark_session("Facebook-Gemini-Write")
    try:
        insights_df = spark.createDataFrame(pd.DataFrame(all_rows), schema=FB_INSIGHT_SCHEMA)
        insights_df.cache()
        insights_df.count()

        write_to_s3(
            insights_df,
            table_name="analytics.fb_fct_post_insights",
            layer="analytics",
            mode="append",
            partition_cols=["ingestion_date", "page_name"],
            path_override="analytics/facebook/fct_post_insights",
        )
        logger.info(f"[conversation_job] wrote {len(all_rows)} insight rows to fct_post_insights")

        # Rebuild weekly agg từ toàn bộ history để LAG window chính xác
        all_insights = spark.read.parquet(f"s3a://{_BUCKET}/analytics/facebook/fct_post_insights/")
        agg_weekly = build_agg_model_issue_weekly(all_insights)
        write_to_s3(
            agg_weekly,
            table_name="analytics.fb_agg_model_issue_weekly",
            layer="analytics",
            mode="overwrite",
            partition_cols=["week_start", "model_entity"],
            path_override="analytics/facebook/agg_model_issue_weekly",
        )
        logger.info("[conversation_job] wrote agg_model_issue_weekly")
        insights_df.unpersist()
    finally:
        spark.stop()
