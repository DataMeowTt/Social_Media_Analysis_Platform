from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request

from src.facebook.analytics.conversation.schemas.insight_schema import VALID_ASPECTS, VALID_MODELS
from src.utils.logger import get_logger

logger = get_logger(__name__)

_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"

_PROMPT = """\
Bạn là chuyên gia phân tích cộng đồng chủ xe VinFast tại Việt Nam.
Cho một thread thảo luận trên Facebook, hãy extract các ý kiến về xe VinFast.

Với mỗi cặp (model_entity, aspect) được đề cập:
- model_entity: PHẢI là một trong: "VF3", "VF5", "VF6", "VF7", "VF8", "VF9", "general"
  (dùng "general" nếu comment nói về VinFast chung, không chỉ rõ model cụ thể)
- aspect: PHẢI là một trong: "pin", "sac", "bao_hanh", "phan_mem",
  "van_hanh", "gia", "loi_ky_thuat", "noi_that", "ket_noi"
- sentiment: "positive" | "negative" | "neutral" | "mixed"
- intensity: "strong" | "moderate" | "mild"
- mention_count: số comments đề cập cặp này
- evidence: 1 quote đại diện, tối đa 150 ký tự tiếng Việt

Quy tắc:
- Chỉ extract những gì được đề cập trực tiếp, không suy diễn
- Bỏ qua spam, emoji-only, off-topic, hoặc nội dung không liên quan tới xe
- Nếu một aspect có ý kiến trái chiều → dùng sentiment "mixed"
- Trả về ONLY một JSON array hợp lệ, không markdown fences

Thread ({n} comments):
{conversation_text}

JSON array:"""


def _format_conversation(comments: list[dict]) -> str:
    lines = []
    for c in comments:
        tag  = "[Root]" if not c.get("is_reply") else "[Reply]"
        text = (c.get("text") or "")[:300]
        lines.append(f'{tag} {c.get("author_name", "")}: "{text}"')
    return "\n".join(lines)


def _call_gemini(prompt: str) -> str:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    model   = os.environ.get("MODEL", "gemini-2.5-flash-lite")
    url     = _API_URL.format(model=model, key=api_key)
    body    = json.dumps({"contents": [{"parts": [{"text": prompt}]}]}).encode()
    req     = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode())
    return data["candidates"][0]["content"]["parts"][0]["text"]


def _parse_response(raw: str) -> list[dict]:
    match = re.search(r"\[.*\]", raw, re.DOTALL)
    if not match:
        return []
    try:
        items = json.loads(match.group())
    except (json.JSONDecodeError, ValueError):
        return []

    result = []
    for item in items:
        model_entity = str(item.get("model_entity", "")).strip()
        aspect       = str(item.get("aspect", "")).strip()
        if model_entity not in VALID_MODELS or aspect not in VALID_ASPECTS:
            continue
        result.append({
            "model_entity":  model_entity,
            "aspect":        aspect,
            "sentiment":     item.get("sentiment", "neutral"),
            "intensity":     item.get("intensity", "mild"),
            "mention_count": int(item.get("mention_count", 1)),
            "evidence":      str(item.get("evidence", ""))[:200],
        })
    return result


def extract_insights_for_post(comments: list[dict]) -> list[dict]:
    conv_text = _format_conversation(comments)
    prompt    = _PROMPT.format(n=len(comments), conversation_text=conv_text)
    try:
        raw      = _call_gemini(prompt)
        insights = _parse_response(raw.strip())
    except Exception as exc:
        logger.error(f"[gemini_extractor] Gemini error: {exc}")
        insights = []
    logger.info(f"[gemini_extractor] {len(insights)} insights from {len(comments)} comments")
    time.sleep(3)
    return insights
