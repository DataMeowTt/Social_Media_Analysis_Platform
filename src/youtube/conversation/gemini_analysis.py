import gc
import gzip
import json
from datetime import datetime, timezone

import pandas as pd

from src.storage.s3.uploader import write_to_s3
from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.utils.session import create_spark_session, get_s3_client
from src.youtube.conversation.gemini_extractor import extract_insights_for_thread
from src.youtube.conversation.insight_merger import merge_insights
from src.youtube.conversation.schemas.insight_schema import INSIGHT_SCHEMA

logger = get_logger(__name__)
_config = load_config()
_BUCKET = _config["s3"]["bucket_name"]


def run_gemini_analysis(youtube_id: str) -> None:
    s3_key = f"analytics/youtube/conversations/{youtube_id}/threads.json.gz"
    obj = get_s3_client().get_object(Bucket=_BUCKET, Key=s3_key)
    conversations = json.loads(gzip.decompress(obj["Body"].read()).decode())
    logger.info(f"Loaded {len(conversations)} conversations")

    if not conversations:
        logger.info("No conversations to analyze")
        return

    analyzed_at = datetime.now(timezone.utc)
    all_rows = []
    total = len(conversations)

    for idx, conv in enumerate(conversations, start=1):
        thread_id = conv["thread_id"]
        logger.info(f"[gemini] Thread {idx}/{total} — {thread_id} ({conv['brand_comment_count']} comments)")

        raw = extract_insights_for_thread(conv["comments"])
        merged = merge_insights(raw, thread_id, youtube_id, conv["brand_comment_count"])
        for row in merged:
            row["analyzed_at"] = analyzed_at
            row["year"] = analyzed_at.year
            row["month"] = analyzed_at.month

        all_rows.extend(merged)
        logger.info(f"[gemini] Thread {idx}/{total} done — {len(merged)} insights, total so far: {len(all_rows)}")
        gc.collect()

    if not all_rows:
        logger.info("No insights generated")
        return

    spark = create_spark_session("YouTube-Gemini-Insights")
    try:
        insights_df = spark.createDataFrame(pd.DataFrame(all_rows), schema=INSIGHT_SCHEMA)
        write_to_s3(
            insights_df,
            table_name="analytics.fct_thread_insights",
            layer="analytics",
            mode="overwrite",
            partition_cols=["year", "month"],
            path_override="analytics/youtube/thread_insights",
        )
        logger.info(f"Saved {len(all_rows)} insight rows")
    finally:
        spark.stop()
