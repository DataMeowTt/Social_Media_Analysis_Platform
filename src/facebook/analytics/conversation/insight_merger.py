from __future__ import annotations

from datetime import date, datetime

from src.facebook.analytics.conversation.schemas.insight_schema import VALID_ASPECTS, VALID_MODELS
from src.utils.logger import get_logger

logger = get_logger(__name__)

_VALID_SENTIMENTS = {"positive", "negative", "neutral", "mixed"}
_VALID_INTENSITIES = {"strong", "moderate", "mild"}


def merge_insights(
    raw_insights: list[dict],
    post_id: str,
    page_name: str,
    ingestion_date: date,
    analyzed_at: datetime,
) -> list[dict]:
    rows = []
    for item in raw_insights:
        model_entity = item.get("model_entity", "")
        aspect       = item.get("aspect", "")
        sentiment    = item.get("sentiment", "neutral")
        intensity    = item.get("intensity", "mild")

        if model_entity not in VALID_MODELS:
            logger.warning(f"[insight_merger] invalid model_entity={model_entity!r} — skipping")
            continue
        if aspect not in VALID_ASPECTS:
            logger.warning(f"[insight_merger] invalid aspect={aspect!r} — skipping")
            continue
        if sentiment not in _VALID_SENTIMENTS:
            sentiment = "neutral"
        if intensity not in _VALID_INTENSITIES:
            intensity = "mild"

        rows.append({
            "post_id":        post_id,
            "page_name":      page_name,
            "model_entity":   model_entity,
            "aspect":         aspect,
            "sentiment":      sentiment,
            "intensity":      intensity,
            "mention_count":  max(int(item.get("mention_count", 1)), 1),
            "evidence":       str(item.get("evidence", ""))[:200],
            "ingestion_date": ingestion_date,
            "analyzed_at":    analyzed_at,
        })
    return rows
