from __future__ import annotations

from collections import defaultdict

from src.utils.logger import get_logger

logger = get_logger(__name__)

_INTENSITY_RANK = {"strong": 3, "moderate": 2, "mild": 1}
_ENTITY_NORMALIZE = {"ios": "iphone"}


def _majority_sentiment(sentiments: list[str]) -> str:
    counts: dict[str, int] = defaultdict(int)
    for s in sentiments:
        counts[s] += 1
    top = max(counts.values())
    winners = [s for s, c in counts.items() if c == top]
    return "mixed" if len(winners) > 1 else winners[0]


def merge_insights(
    raw: list[dict],
    thread_id: str,
    video_id: str,
    brand_comment_count: int,
) -> list[dict]:
    grouped: dict[tuple, list[dict]] = defaultdict(list)
    for item in raw:
        entity = _ENTITY_NORMALIZE.get(item["entity"], item["entity"])
        grouped[(entity, item["aspect"])].append({**item, "entity": entity})

    merged = []
    for (entity, aspect), items in grouped.items():
        best = max(items, key=lambda x: _INTENSITY_RANK.get(x["intensity"], 0))
        merged.append({
            "thread_id":           thread_id,
            "video_id":            video_id,
            "entity":              entity,
            "aspect":              aspect,
            "sentiment":           _majority_sentiment([i["sentiment"] for i in items]),
            "intensity":           best["intensity"],
            "mention_count":       sum(i["mention_count"] for i in items),
            "evidence":            best["evidence"],
            "brand_comment_count": brand_comment_count,
        })

    logger.info(f"[{thread_id}] {len(raw)} raw → {len(merged)} merged insights")
    return merged
