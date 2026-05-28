from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request

from src.utils.logger import get_logger

logger = get_logger(__name__)

_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"

_VALID_ENTITIES = {"iphone", "ios", "android"}
_ENTITY_NORMALIZE = {"ios": "iphone"}


_PROMPT = """\
You are an expert product analyst comparing iOS (iPhone) and Android smartphones. \
Given a YouTube comment thread, extract opinions about iOS/iPhone and Android ONLY.

For each distinct (entity, aspect) pair discussed:
- entity: MUST be exactly one of: "iphone", "android" — treat iOS/iPhone as "iphone", ignore all other brands
- aspect: the product dimension. Use "overall" for general opinions with no specific dimension. \
Other valid aspects: "battery", "performance", "price", "camera", "design", "software", \
"durability", "value_for_money", "ecosystem", "customer_service"
- sentiment: "positive" | "negative" | "neutral" | "mixed"
- intensity: "strong" | "moderate" | "mild"
- mention_count: number of comments addressing this aspect
- evidence: the exact phrase (≤10 words) from the comment that justifies this aspect

Rules:
- Only extract aspects about iphone/android — skip Samsung, Pixel, or any other brand
- An aspect is valid ONLY if you can quote specific words from the comment that name or clearly describe that dimension.
  Examples of valid triggers: "expensive"/"cheap" → price; "slow"/"fast" → performance; "ugly"/"looks" → design; "limited apps"/"open" → ecosystem
  Examples of invalid inference: "boring" does NOT imply design or price; "limited" alone does NOT imply ecosystem — use "overall" instead
- Sentiment MUST match the tone of the evidence phrase: words like "boring", "limited", "bad", "hate", "worst" → negative; "great", "love", "best", "amazing" → positive
- If an aspect has opposing views across comments, use sentiment "mixed"
- Ignore spam, emojis-only, or off-topic comments
- Return ONLY a valid JSON array, no markdown fences

Thread ({n} comments):
{conversation_text}

JSON array:"""


def _format_conversation(comments: list[dict]) -> str:
    lines = []
    for c in comments:
        tag = "[Root]" if c.get("is_root") else "[Reply]"
        brand = f"[{c['brand_mentioned']}]" if c.get("brand_mentioned") else ""
        text = (c.get("text") or "")[:300]
        lines.append(f'{tag}{brand} {c.get("author_name", "")}: "{text}"')
    return "\n".join(lines)


def _call_gemini(prompt: str) -> str:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    model = os.environ.get("MODEL", "gemini-2.5-flash-lite")
    logger.info(f"Calling Gemini model: {model}")
    url = _API_URL.format(model=model, key=api_key)
    body = json.dumps({"contents": [{"parts": [{"text": prompt}]}]}).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode())
    return data["candidates"][0]["content"]["parts"][0]["text"]


def _parse_response(raw: str) -> list[dict]:
    match = re.search(r"\[.*\]", raw, re.DOTALL)
    if not match:
        return []
    try:
        items = json.loads(match.group())
        result = []
        for item in items:
            entity = str(item.get("entity", "")).lower().strip()
            aspect = str(item.get("aspect", "")).lower().strip()
            if not entity or not aspect or entity not in _VALID_ENTITIES:
                continue
            entity = _ENTITY_NORMALIZE.get(entity, entity)
            result.append({
                "entity":        entity,
                "aspect":        aspect,
                "sentiment":     item.get("sentiment", "neutral"),
                "intensity":     item.get("intensity", "mild"),
                "mention_count": int(item.get("mention_count", 1)),
                "evidence":      str(item.get("evidence", ""))[:200],
            })
        return result
    except (json.JSONDecodeError, ValueError, TypeError):
        return []


def extract_insights_for_thread(comments: list[dict]) -> list[dict]:
    conv_text = _format_conversation(comments)
    prompt = _PROMPT.format(n=len(comments), conversation_text=conv_text)
    try:
        raw = _call_gemini(prompt)
        insights = _parse_response(raw.strip())
    except Exception as e:
        logger.error(f"Gemini error: {e}")
        insights = []
    logger.info(f"{len(insights)} insights extracted from {len(comments)} comments")
    time.sleep(3)
    return insights
